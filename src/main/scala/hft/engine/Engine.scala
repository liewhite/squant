package hft.engine

import hft.domain.*
import hft.exchange.{ExchangeClient, ExchangeConnector, SubscriptionKind}
import hft.messaging.{EventBus, EventData, IncomeEvent}
import hft.strategy.{OutcomeEvent, Strategy}
import org.slf4j.LoggerFactory
import ox.{Ox, fork}

/** 一个交易所的接入单元: REST 客户端 + WS 连接器 */
final case class ExchangeGateway(client: ExchangeClient, connector: ExchangeConnector)

/** 引擎：装配并管理所有组件的生命周期。
  *
  * 事件流：
  * {{{
  * Connector (WS) ──┐
  * Clock ───────────┼─> incomeBus ─> Executor (Strategy + StateManager) ─> outcomeBus ─> OutcomeProcessor ─> REST
  * 执行回流 <────────┘                                                                          │
  *    └──────────────────────────────── OrderUpdate (撤单确认/下单失败) <───────────────────────┘
  * }}}
  *
  * 所有组件都是引擎所在 Ox 作用域内的虚拟线程 fork，任一组件崩溃将级联终止整个作用域，
  * 对应参考实现中 spawn_link + 级联退出的监督语义。
  */
final class Engine private (
    clients: Map[Exchange, ExchangeClient],
    connectors: Map[Exchange, ExchangeConnector],
    symbolMetas: Map[(Exchange, Symbol), SymbolMeta],
    incomeBus: EventBus[IncomeEvent],
    outcomeBus: EventBus[OutcomeEvent],
)(using Ox):
  private val logger = LoggerFactory.getLogger(classOf[Engine])

  def addStrategy(strategy: Strategy): Unit = addStrategies(Vector(strategy))

  /** 批量添加策略。
    *
    * 启动顺序保证 (与参考实现一致)：
    *   1. 创建 Executor 并订阅 income 总线 —— 之后发布的事件不会丢
    *   2. REST 查询初始持仓并发布 —— 避免策略基于缺失仓位决策；
    *      交易所未返回的 symbol 显式推 size=0，保证 SymbolState 一定收到初始值
    *   3. REST 查询现有挂单并发布 —— 策略接管启动前的遗留订单
    *   4. 向交易所订阅行情 —— 市场数据从此处开始流动
    */
  def addStrategies(strategies: Seq[Strategy]): Unit =
    if strategies.isEmpty then return

    // 1. 创建 Executor，订阅 income 总线
    strategies.foreach { strategy =>
      Executor(strategy, symbolMetas, outcomeBus).run(incomeBus.subscribe())
    }

    // 收集所有策略涉及的订阅
    val allSubscriptions: Set[(Exchange, SubscriptionKind)] =
      strategies.toSet.flatMap { (s: Strategy) =>
        s.publicStreams.toSet.flatMap { (exchange, kinds) => kinds.map((exchange, _)) }
      }
    val exchangeSymbols: Set[(Exchange, Symbol)] =
      allSubscriptions.map((exchange, kind) => (exchange, kind.subscribedSymbol))

    // 2. 初始持仓
    publishInitialPositions(exchangeSymbols)

    // 3. 现有挂单
    publishExistingPendingOrders(exchangeSymbols)

    // 4. 订阅行情
    allSubscriptions.groupMap(_._1)(_._2).foreach { (exchange, kinds) =>
      connectors.get(exchange) match
        case Some(connector) => connector.subscribe(kinds)
        case None            => logger.error(s"No connector for exchange $exchange, subscriptions skipped")
    }

    logger.info(s"${strategies.size} strategies added")

  private def publishInitialPositions(exchangeSymbols: Set[(Exchange, Symbol)]): Unit =
    exchangeSymbols.groupMap(_._1)(_._2).foreach { (exchange, symbols) =>
      clients.get(exchange).foreach { client =>
        client.fetchPositions() match
          case Left(e) =>
            logger.warn(s"Failed to fetch initial positions on $exchange, proceeding without: ${e.message}")
          case Right(positions) =>
            val bySymbol = positions.map(p => p.symbol -> p).toMap
            symbols.foreach { symbol =>
              val pos = bySymbol.getOrElse(symbol, Position.empty(exchange, symbol))
              logger.info(s"Initial position loaded: $exchange $symbol size=${pos.size}")
              incomeBus.publish(IncomeEvent.local(EventData.PositionUpdate(pos)))
            }
      }
    }

  private def publishExistingPendingOrders(exchangeSymbols: Set[(Exchange, Symbol)]): Unit =
    exchangeSymbols.foreach { (exchange, symbol) =>
      clients.get(exchange).foreach { client =>
        client.fetchPendingOrders(symbol) match
          case Left(e) =>
            logger.warn(s"Failed to fetch pending orders on $exchange $symbol, proceeding without: ${e.message}")
          case Right(updates) =>
            if updates.nonEmpty then
              logger.info(s"Fetched ${updates.size} existing pending orders: $exchange $symbol")
            updates.foreach { update =>
              // REST 返回的数量是合约张数，转换为币本位
              val converted = symbolMetas.get((exchange, symbol)) match
                case Some(meta) =>
                  update.copy(
                    quantity = meta.qtyToCoin(update.quantity),
                    filledQuantity = meta.qtyToCoin(update.filledQuantity),
                  )
                case None => update
              incomeBus.publish(IncomeEvent.local(EventData.OrderUpdated(converted)))
            }
      }
    }

object Engine:
  private val logger = LoggerFactory.getLogger(classOf[Engine])

  /** 启动引擎：预加载交易对元数据 (失败即终止启动)、装配事件总线、
    * 启动信号处理器 / 时钟 / 各交易所连接器。
    */
  def start(
      gateways: Seq[ExchangeGateway],
      dryRun: Boolean = false,
      clockIntervalMs: Long = 1000,
  )(using Ox): Engine =
    val clients = gateways.map(g => g.client.exchange -> g.client).toMap
    val connectors = gateways.map(g => g.connector.exchange -> g.connector).toMap

    // 预加载所有交易所的 symbol metas，任一失败 → 启动失败快速退出
    val symbolMetas: Map[(Exchange, Symbol), SymbolMeta] =
      clients.values.flatMap { client =>
        client.fetchAllSymbolMetas() match
          case Right(metas) =>
            logger.info(s"Preloaded ${metas.size} symbol metas from ${client.exchange}")
            metas.map(m => (m.exchange, m.symbol) -> m)
          case Left(e) =>
            throw IllegalStateException(s"Failed to preload symbol metas from ${client.exchange}: ${e.message}")
      }.toMap

    val incomeBus = EventBus[IncomeEvent]()
    val outcomeBus = EventBus[OutcomeEvent]()

    OutcomeProcessor(clients, incomeBus, dryRun).run(outcomeBus.subscribe())

    // 时钟: 周期性发布 Clock 事件 (驱动订单超时清理等定时任务)
    fork {
      while true do
        Thread.sleep(clockIntervalMs)
        incomeBus.publish(IncomeEvent.local(EventData.Clock))
    }

    connectors.values.foreach(_.start(incomeBus))

    logger.info("Engine started")
    Engine(clients, connectors, symbolMetas, incomeBus, outcomeBus)
