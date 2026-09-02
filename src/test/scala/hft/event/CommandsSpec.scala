package hft.event

import hft.TestUnits.given
import hft.domain.*
import hft.event.Commands.OutcomeEvent

/** 指令面协议的不变量：一条下单指令必须非空且同属一个交易所。 */
class CommandsSpec extends munit.FunSuite:
  private def order(exchange: Exchange) = Order(
    id = "",
    exchange = exchange,
    symbol = "BTCUSDT",
    side = Side.Long,
    orderType = OrderType.Limit(Price(100.0), TimeInForce.GTC),
    quantity = Coin(1.0),
    reduceOnly = false,
    clientOrderId = "",
  )

  test("PlaceOrders 为空 -> 构造即拒绝 (没有交易所可路由)"):
    val e = intercept[IllegalArgumentException](OutcomeEvent.PlaceOrders(Vector.empty, "空信号"))
    assert(e.getMessage.contains("不能为空"), e.getMessage)

  test("PlaceOrders 混入第二个交易所 -> 构造即拒绝, 不静默按 head 路由"):
    // targetExchange 取的是 orders.head.exchange: 混所的后果是那些单去了别的交易所,
    // 而这是唯一的症状。不变量因此必须由构造本身保证, 不能只写在注释里。
    val e = intercept[IllegalArgumentException](
      OutcomeEvent.PlaceOrders(Vector(order(Exchange.Binance), order(Exchange.Okx)), "跨所")
    )
    assert(e.getMessage.contains("必须同属一个交易所"), e.getMessage)

  test("同所多单 -> 允许, targetExchange 即该交易所"):
    val ev = OutcomeEvent.PlaceOrders(Vector(order(Exchange.Okx), order(Exchange.Okx)), "两条腿")
    assertEquals(ev.targetExchange, Exchange.Okx)
