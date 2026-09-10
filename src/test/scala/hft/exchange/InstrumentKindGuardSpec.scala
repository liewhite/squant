package hft.exchange

import hft.domain.*
import hft.exchange.binance.{BinanceClient, BinanceCredentials}
import hft.exchange.bybit.{BybitClient, BybitCredentials}
import hft.exchange.okx.{OkxClient, OkxCredentials}

import sttp.client4.testing.BackendStub

import java.util.concurrent.atomic.AtomicInteger

/** 适配层的**品种守卫**：不支持的品种一律在发出请求之前失败。
  *
  * 这一条必须有测试，因为它的失效是沉默的：把一张期权单发到永续端点上，要么被交易所拒
  * （白跑一趟），要么撞上一个同名的永续合约。而三家的守卫是三份各自独立的判断 ——
  * 上一版就漏了最危险的那条路径（`placeOrder` 没加守卫，`cancelOrder` 加了），
  * 全量测试照过。
  *
  * 断言"没有发出请求"而不只是"抛了异常"：守卫的意义正是**别打出去**。
  */
class InstrumentKindGuardSpec extends munit.FunSuite:

  /** 数请求次数的后端 —— 守卫生效时它一次都不该被调用。 */
  private def countingBackend(calls: AtomicInteger) =
    BackendStub.synchronous.whenAnyRequest.thenRespondF { _ =>
      calls.incrementAndGet()
      throw IllegalStateException("守卫失效: 请求已经发出去了")
    }

  private def optionOf(exchange: Exchange): Instrument =
    Instrument.option(exchange, "ETH-26SEP25-3000-C-USDT")

  private def optionOrder(exchange: Exchange): ExchangeOrder =
    ExchangeOrder(
      exchange = exchange,
      symbol = optionOf(exchange).symbol,
      side = Side.Long,
      orderType = OrderType.Limit(Price(10.0), TimeInForce.GTC),
      quantity = Contracts(1.0),
      reduceOnly = false,
      clientOrderId = "c1",
      kind = InstrumentKind.Option,
    )

  /** 三个写/读入口都要挡 —— 漏一个就是一条能把单发出去的路。 */
  private def checkAllEntries(name: String)(build: sttp.client4.SyncBackend => TradingClient): Unit =
    val calls = AtomicInteger(0)
    val client = build(countingBackend(calls))
    val instrument = optionOf(client.exchange)

    val place = intercept[IllegalArgumentException](client.placeOrder(optionOrder(client.exchange)))
    assert(place.getMessage.contains("只支持"), s"$name placeOrder: ${place.getMessage}")

    val cancel = intercept[IllegalArgumentException](client.cancelOrder(instrument, OrderRef.ByClientId("c1")))
    assert(cancel.getMessage.contains("只支持"), s"$name cancelOrder: ${cancel.getMessage}")

    val pending = intercept[IllegalArgumentException](client.fetchPendingOrders(instrument))
    assert(pending.getMessage.contains("只支持"), s"$name fetchPendingOrders: ${pending.getMessage}")

    assertEquals(calls.get(), 0, s"$name: 守卫应在发出请求之前挡下, 实际发出了 ${calls.get()} 次")

  test("Binance: 期权标的在三个入口都被挡下, 且没有发出请求"):
    checkAllEntries("Binance")(b => BinanceClient.trading(b, BinanceCredentials("k", "s")))

  test("Bybit: 期权标的在三个入口都被挡下, 且没有发出请求"):
    checkAllEntries("Bybit")(b => BybitClient.trading(b, BybitCredentials("k", "s")))

  test("OKX: 期权标的在三个入口都被挡下, 且没有发出请求"):
    // OKX 的 codec 已经能为期权拼出正确的 instId, 但响应侧 (fetchPositions / 私有流 /
    // 挂单查询的 fromOkx) 还只认永续 —— 只放开请求侧的话, 单能发出去然后全程没有回报。
    // 所以请求侧一起拒绝, 直到响应侧接通。
    checkAllEntries("OKX")(b => OkxClient.trading(b, OkxCredentials("k", "s", "p")))

  test("永续标的照常放行 —— 守卫挡的是品种, 不是所有单"):
    val calls = AtomicInteger(0)
    val client = BybitClient.trading(countingBackend(calls), BybitCredentials("k", "s"))
    val perp = Instrument.perp(Exchange.Bybit, "ETHUSDT")
    // stub 抛出的异常被客户端的错误处理归成 Left —— 那正说明它**已经走到发请求这一步**,
    // 守卫没有误挡。这里要的不是返回值, 是"请求确实发出去了"这个事实。
    client.fetchPendingOrders(perp)
    assertEquals(calls.get(), 1, "永续应该正常走到发请求")
