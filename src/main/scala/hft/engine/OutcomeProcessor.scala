package hft.engine

import hft.actor.{Actor, ActorContext}
import hft.domain.*
import hft.exchange.ExchangeClient
import hft.event.{AnyEvent, Event, Interest, Topics}
import hft.strategy.{OrderIntent, OutcomeEvent}
import org.slf4j.LoggerFactory

/** 信号处理器：消费策略信号，调用交易所 REST API 执行下单/撤单。
  *
  * 每个 REST 调用 fork 独立虚拟线程执行，互不阻塞。
  *
  * 错误处理 (fail-fast)：
  *   - 交易所明确拒绝 (HTTP 4xx，订单确定未成立) 是正常业务结果，
  *     以 OrderUpdate(Error/Rejected) 事件回流策略
  *   - 网络错误 / 超时 / 5xx —— 订单是否已到达交易所**不确定**，本地状态无法保证正确，
  *     直接抛错终止引擎 (重启后由启动对齐恢复一致)
  */
final class OutcomeProcessor(
    clients: Map[Exchange, ExchangeClient],
    dryRun: Boolean,
    /** 本出口负责的账户 (真实交易所出口即 [[AccountId.Live]])。无默认值，理由同 [[Executor]] */
    account: AccountId,
) extends Actor:
  private val logger = LoggerFactory.getLogger(classOf[OutcomeProcessor])

  /** REST 调用要各自 fork、失败要回流事件，两者都得经 context。
    * 由 [[hft.actor.ActorSystem]] 在 onStart 时注入，之后只读。 */
  @volatile private var ctx: ActorContext = scala.compiletime.uninitialized

  override def name: String = s"outcome-processor@$account"

  /** 只订阅本出口负责的账户。
    *
    * "这条信号该由谁执行"因此由投递层回答：实盘信号进这里、影子盘信号进它自己的柜台，
    * 两个出口互不知情。若改成全量订阅再各自过滤，新增一类账户时两处都不会编译失败，
    * 失效方式是静默双执行或静默不执行。
    */
  override def interests: Set[Interest] = Set(Interest.Keyed(OrderIntent, Set(account)))

  override def onStart(context: ActorContext): Unit =
    ctx = context
    if dryRun then logger.warn("OutcomeProcessor started in DRY-RUN mode (orders will NOT be placed)")
    else logger.info("OutcomeProcessor started")

  override def onEvent(event: AnyEvent, now: Timestamp): Vector[AnyEvent] =
    event.as(OrderIntent).foreach(intent => handle(intent.outcome))
    Vector.empty

  private def handle(signal: OutcomeEvent): Unit = signal match
    case OutcomeEvent.PlaceOrders(orders, comment) =>
      // 关联订单独立并行下单：IOC 订单本身接受部分成交，敞口由策略层 rebalance 兜底
      orders.foreach(placeOrder(_, comment))
    case OutcomeEvent.CancelOrder(exchange, symbol, ref) =>
      cancelOrder(exchange, symbol, ref)

  private def placeOrder(order: Order, comment: String): Unit =
    if dryRun then
      logger.warn(s"[DRY-RUN] order NOT placed: ${describe(order)} signal=$comment")
      // dry-run 等价于确定性的"未下单"，以 Error 事件回流清理 pending
      publishOrderError(order, "dry-run: order not placed")
    else
      val client = requireClient(order.exchange)
      logger.info(s"Placing order: ${describe(order)} signal=$comment")
      ctx.fork {
        client.placeOrder(order) match
          case Right(orderId) =>
            // 订单确认 (Pending/Filled) 以私有流推送为准，这里只记录
            logger.info(
              s"Order accepted: ${order.exchange} ${order.symbol} orderId=$orderId clientOrderId=${order.clientOrderId}"
            )
          case Left(e @ ExchangeError.Http(status, _)) if status == 429 || status == 418 =>
            // 限频/封禁: 说明"订单生命周期自然限速"的假设已被打破，
            // 按拒单回流会形成"拒单->重挂->更多请求"的重试风暴，必须终止
            throw IllegalStateException(s"Rate limited by exchange, aborting: ${describe(order)} error=${e.message}")
          case Left(e @ ExchangeError.Http(status, _)) if status >= 400 && status < 500 =>
            // 交易所明确拒绝，订单确定未成立 -> 回流策略
            logger.warn(s"Order rejected: ${describe(order)} error=${e.message}")
            publishOrderError(order, e.message)
          case Left(e) =>
            // 网络/超时/5xx: 订单是否成立不确定，本地状态无法保证正确
            throw IllegalStateException(s"Place order outcome UNKNOWN, aborting: ${describe(order)} error=${e.message}")
      }
      ()

  private def cancelOrder(exchange: Exchange, symbol: Symbol, ref: OrderRef): Unit =
    if dryRun then logger.warn(s"[DRY-RUN] CancelOrder NOT sent: $exchange $symbol ${ref.raw}")
    else
      val client = requireClient(exchange)
      logger.info(s"Cancelling order: $exchange $symbol ${ref.raw}")
      ctx.fork {
        client.cancelOrder(symbol, ref) match
          case Right(()) =>
            // 终态 (Cancelled) 以私有流推送为准
            logger.info(s"Cancel accepted: $exchange $symbol ${ref.raw}")
          case Left(ExchangeError.OrderNotFound(reason)) =>
            // 订单已成交/已撤销，终态同样由私有流推送，撤单失败非致命
            logger.info(s"Order already gone: $exchange $symbol ${ref.raw} ($reason)")
          case Left(e) =>
            throw IllegalStateException(s"Cancel outcome UNKNOWN, aborting: $exchange $symbol ${ref.raw} error=${e.message}")
      }
      ()

  /** 策略引用了未配置的交易所是装配错误，立即终止 */
  private def requireClient(exchange: Exchange): ExchangeClient =
    clients.getOrElse(exchange, throw IllegalStateException(s"No client configured for exchange $exchange"))

  /** 确定性的下单失败以 OrderUpdate(AccountId.Live, Error) 回流，驱动 pending order 清理 */
  private def publishOrderError(order: Order, reason: String): Unit =
    val update = OrderUpdate(
      account = account,
      orderId = "",
      clientOrderId = Some(order.clientOrderId),
      exchange = order.exchange,
      symbol = order.symbol,
      side = order.side,
      status = OrderStatus.Error(reason),
      price = 0.0,
      quantity = 0.0,
      filledQuantity = 0.0,
      fillSize = 0.0,
      timestamp = nowMs,
    )
    ctx.publish(Event.local(Topics.OrderUpdate, update))

  private def describe(order: Order): String =
    s"${order.exchange} ${order.symbol} ${order.side} ${order.orderType} qty=${order.quantity} " +
      s"reduceOnly=${order.reduceOnly} clientOrderId=${order.clientOrderId}"
