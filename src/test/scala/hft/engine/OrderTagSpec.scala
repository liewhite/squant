package hft.engine

import hft.TestUnits.given
import hft.domain.*
import hft.event.Commands.{OrderIntent, OutcomeEvent}
import hft.event.{AnyEvent, Event, Topics}
import hft.strategy.{Strategy, StrategyContext, StrategyHandlers}

import scala.collection.mutable

/** 订单标注 (`Order.tag`) 的往返：策略下单时标上，回报回来时认得出。
  *
  * 这消灭的是"策略认不出自己的回报"——clientOrderId 由框架在处理器返回之后才生成，策略
  * 手里那个是空串，拿它匹配回报一条都匹配不上 (见 [[hft.domain.Order.tag]])。
  */
class OrderTagSpec extends munit.FunSuite:
  private val ex = Exchange.Binance
  private val symbol = "BTCUSDT"
  private val instrument = Instrument(ex, symbol)

  /** 下 `orders` 里的单, 并把每条订单回报看到的 (状态, 标注) 记进 [[seen]]。 */
  private final class TaggingStrategy(orders: Vector[Order]) extends Strategy:
    val seen: mutable.ArrayBuffer[(OrderStatus, Option[String])] = mutable.ArrayBuffer.empty

    override def handlers: StrategyHandlers = StrategyHandlers.empty
      .market(Topics.Bbo, instrument)((_, ctx, _) => ctx.place(orders, "test"))
      .own(Topics.OrderUpdate) { (update, ctx, _) =>
        seen += (update.status -> ctx.orderTag)
        Vector.empty
      }

  private def order(tag: String, side: Side = Side.Long): Order =
    Order(
      id = "",
      exchange = ex,
      symbol = symbol,
      side = side,
      orderType = OrderType.Limit(50_000.0, TimeInForce.GTC),
      quantity = Coin(0.01),
      reduceOnly = false,
      clientOrderId = "",
      tag = tag,
    )

  private def bbo: AnyEvent =
    Event.local(Topics.Bbo, BBO(ex, symbol, 49_999.0, Coin(1.0), 50_001.0, Coin(1.0), 0L))

  private def updateOf(clientOrderId: String, status: OrderStatus): AnyEvent =
    Event.local(
      Topics.OrderUpdate,
      OrderUpdate(
        AccountId.Live, s"exch-$clientOrderId", Some(clientOrderId), ex, symbol, Side.Long,
        status, 50_000.0, Coin(0.01), Coin(0.0), reduceOnly = false, 0L,
      ),
    )

  /** 跑一次 BBO 让策略下单, 返回框架分配的 clientOrderId (按下单顺序)。 */
  private def placeAndCollectIds(runner: StrategyRunner): Vector[String] =
    runner
      .onEvent(bbo, now = 1L)
      .flatMap(_.as(OrderIntent))
      .flatMap(_.outcome match
        case OutcomeEvent.PlaceOrders(placed, _) => placed.map(_.clientOrderId)
        case _                                   => Vector.empty)

  test("标注跨越下单往返: 回报回来时策略认得出是哪一张"):
    val strategy = TaggingStrategy(Vector(order("entry")))
    val runner = StrategyRunner.backtest(strategy)
    val ids = placeAndCollectIds(runner)
    assertEquals(ids.size, 1)
    assertNotEquals(ids.head, "", "clientOrderId 由框架生成, 策略给的空串应被覆盖")

    runner.onEvent(updateOf(ids.head, OrderStatus.Pending), now = 2L)
    assertEquals(strategy.seen.toVector, Vector(OrderStatus.Pending -> Some("entry")))

  test("**终态**回报仍带标注 —— 挂单登记此刻已被移除, 这正是要害"):
    // 终态时 InstrumentState 会把这张单从挂单登记里删掉, 而策略恰恰在终态里做事 (结算、重挂、
    // 平腿)。取标注若发生在应用事件之后, 这里拿到的就是 None —— 没有报错, 只是策略从此
    // 认不出自己的单。顺序封在 StrategyRunner.observe 里, 本用例钉住它。
    val strategy = TaggingStrategy(Vector(order("entry")))
    val runner = StrategyRunner.backtest(strategy)
    val id = placeAndCollectIds(runner).head

    runner.onEvent(updateOf(id, OrderStatus.Filled), now = 2L)
    assertEquals(strategy.seen.toVector, Vector(OrderStatus.Filled -> Some("entry")))
    assert(
      runner.state.instrumentState(instrument).exists(_.pendingOrders.isEmpty),
      "终态之后挂单登记应已清空 —— 标注是在清空之前取出的",
    )

  test("同一标的上的多张单各认各的 —— 这是本机制解开的表达力"):
    // 从前只能靠"同一时刻同一标的只有一张在途单"这条不变量认单, 多张可区分的单写不出来。
    val strategy = TaggingStrategy(Vector(order("rich", Side.Short), order("cheap", Side.Long)))
    val runner = StrategyRunner.backtest(strategy)
    val ids = placeAndCollectIds(runner)
    assertEquals(ids.size, 2)
    assertEquals(ids.distinct.size, 2, "两张单必须拿到不同的 clientOrderId")

    runner.onEvent(updateOf(ids(1), OrderStatus.Filled), now = 2L)
    runner.onEvent(updateOf(ids(0), OrderStatus.Cancelled), now = 3L)
    assertEquals(
      strategy.seen.toVector,
      Vector(OrderStatus.Filled -> Some("cheap"), OrderStatus.Cancelled -> Some("rich")),
      "回报顺序与下单顺序无关, 各自带各自的标注",
    )

  test("未标注的单给 None —— 空标注不是一种标注"):
    val strategy = TaggingStrategy(Vector(order("")))
    val runner = StrategyRunner.backtest(strategy)
    val id = placeAndCollectIds(runner).head

    runner.onEvent(updateOf(id, OrderStatus.Pending), now = 2L)
    assertEquals(strategy.seen.toVector, Vector(OrderStatus.Pending -> None))

  test("接管的既有挂单给 None —— 标注只活在下单那个进程的内存里"):
    // 重启后从交易所接管上个进程留下的单: 回报带 clientOrderId, 但本地挂单登记里没有它。
    // 策略若按标注分派, 必须能处理这一支 —— 接管单一直是这样, 与本机制无关。
    val strategy = TaggingStrategy(Vector.empty)
    val runner = StrategyRunner.backtest(strategy)

    runner.onEvent(updateOf("from-previous-process", OrderStatus.Pending), now = 1L)
    assertEquals(strategy.seen.toVector, Vector(OrderStatus.Pending -> None))

  test("回报没带 clientOrderId 给 None —— 那压根不是本策略发的单"):
    // 手工单、交易所自建的 TP/SL 都不带 clOrdId, 它们本来就不进本策略的挂单登记。
    val strategy = TaggingStrategy(Vector(order("entry")))
    val runner = StrategyRunner.backtest(strategy)
    placeAndCollectIds(runner)

    val foreign = Event.local(
      Topics.OrderUpdate,
      OrderUpdate(
        AccountId.Live, "exch-manual", None, ex, symbol, Side.Long,
        OrderStatus.Filled, 50_000.0, Coin(0.01), Coin(0.01), reduceOnly = false, 0L,
      ),
    )
    runner.onEvent(foreign, now = 2L)
    assertEquals(strategy.seen.toVector, Vector(OrderStatus.Filled -> None))

  test("精度拒单回流仍带标注 —— 策略最常在这条路径上重挂"):
    // 柜台对齐精度后收不下的单以 OrderUpdate(Error) 回流, 与交易所明确拒单同一条路径
    // (见 TradingGateway.rejection)。它是与撮合成交不同的生产者, 单独钉一次。
    val strategy = TaggingStrategy(Vector(order("entry")))
    val runner = StrategyRunner.backtest(strategy)
    val id = placeAndCollectIds(runner).head

    val rejected = hft.exchange.TradingGateway.rejection(
      AccountId.Live, ex, order("entry").copy(clientOrderId = id), "below min order size", now = 2L,
    )
    runner.onEvent(rejected, now = 2L)
    assertEquals(strategy.seen.toVector, Vector(OrderStatus.Error("below min order size") -> Some("entry")))

  test("非回报事件没有标注"):
    final class BboWatcher extends Strategy:
      val tags: mutable.ArrayBuffer[Option[String]] = mutable.ArrayBuffer.empty
      override def handlers: StrategyHandlers = StrategyHandlers.empty
        .market(Topics.Bbo, instrument) { (_, ctx, _) => tags += ctx.orderTag; Vector.empty }
    val strategy = BboWatcher()
    StrategyRunner.backtest(strategy).onEvent(bbo, now = 1L)
    assertEquals(strategy.tags.toVector, Vector(None))

  test("标注不发给交易所 —— 它不在 ExchangeOrder 上"):
    // 结构保证: ExchangeOrder 没有这个字段, 所以"标注泄漏到交易所"写不出来。
    // 这条用例存在的意义是把这个事实钉成契约: 有人给 ExchangeOrder 加上 tag 时它会失败。
    val meta = SymbolMeta(ex, symbol, tickSize = 0.1, sizeStep = 0.001, minOrderSize = 0.001, contractSize = 1.0)
    val exchangeOrder = OrderConversion.toExchangeOrder(order("secret"), meta)
    assert(
      !exchangeOrder.toString.contains("secret"),
      "标注是策略进程内的本地事实, 不该出现在发往交易所的订单上",
    )
