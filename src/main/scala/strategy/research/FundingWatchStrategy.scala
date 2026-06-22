package strategy.research

import hft.domain.*
import hft.exchange.SubscriptionKind
import hft.messaging.{EventData, IncomeEvent, StateManager}
import hft.strategy.{OutcomeEvent, Strategy}
import org.slf4j.LoggerFactory

/** 演示策略：监控资金费率，日化费率超过阈值时挂限价单做空收资费。
  *
  * 仅用于演示框架的完整事件流 (行情 -> 状态 -> 信号 -> 下单 -> 订单超时清理)，
  * 不构成任何交易建议。建议配合 dryRun=true 运行。
  *
  * @param orderQty 各 symbol 的下单数量 (币本位)
  */
final class FundingWatchStrategy(
    symbols: Vector[Symbol],
    dailyRateThreshold: Rate,
    orderQty: Map[Symbol, Quantity],
) extends Strategy:
  private val logger = LoggerFactory.getLogger(classOf[FundingWatchStrategy])

  /** 每 N 个 Clock 打印一次状态快照 */
  private val SnapshotEveryTicks = 10
  private var tick = 0

  override def publicStreams: Map[Exchange, Set[SubscriptionKind]] =
    Map(
      Exchange.Binance -> symbols.flatMap { s =>
        Set(SubscriptionKind.BBO(s), SubscriptionKind.FundingRate(s), SubscriptionKind.MarkPrice(s))
      }.toSet
    )

  override def orderTimeoutMs: Long = 5000

  override def onEvent(event: IncomeEvent, state: StateManager): Vector[OutcomeEvent] =
    event.data match
      case EventData.Clock =>
        tick += 1
        if tick % SnapshotEveryTicks == 0 then logSnapshot(state)
        symbols.flatMap(check(_, state))
      case _ => Vector.empty

  private def check(symbol: Symbol, state: StateManager): Option[OutcomeEvent] =
    for
      symbolState <- state.symbolState(symbol)
      funding <- symbolState.fundingRate(Exchange.Binance)
      bbo <- symbolState.bbo(Exchange.Binance)
      if funding.dailyRate > dailyRateThreshold
      if !symbolState.hasPendingOrders
      if symbolState.position(Exchange.Binance).forall(_.isEmpty)
      qty <- orderQty.get(symbol)
    yield
      val order = Order(
        id = "",
        exchange = Exchange.Binance,
        symbol = symbol,
        side = Side.Short,
        orderType = OrderType.Limit(bbo.askPrice, TimeInForce.PostOnly),
        quantity = qty,
        reduceOnly = false,
        clientOrderId = "", // 由 Executor 生成
      )
      val comment = f"funding_short | daily=${funding.dailyRate * 100}%.4f%% | ask=${bbo.askPrice}"
      OutcomeEvent.PlaceOrders(Vector(order), comment)

  private def logSnapshot(state: StateManager): Unit =
    symbols.foreach { symbol =>
      state.symbolState(symbol).foreach { s =>
        val bbo = s.bbo(Exchange.Binance).map(b => f"bid=${b.bidPrice} ask=${b.askPrice}").getOrElse("bbo=?")
        val funding = s
          .fundingRate(Exchange.Binance)
          .map(r => f"funding=${r.rate * 100}%.4f%% daily=${r.dailyRate * 100}%.4f%%")
          .getOrElse("funding=?")
        logger.info(s"[$symbol] $bbo | $funding | pending=${s.pendingOrders.size} pos=${s.positionSize(Exchange.Binance)}")
      }
    }
