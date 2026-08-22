package hft.state

import hft.domain.*
import hft.state.{StateManager, SymbolState}
import hft.event.{Event, Topics}

class StateManagerSpec extends munit.FunSuite:
  private val t0 = 1_700_000_000_000L

  private def newOrder(symbol: Symbol, clientOrderId: String): Order =
    Order(
      id = "",
      exchange = Exchange.Binance,
      symbol = symbol,
      side = Side.Long,
      orderType = OrderType.Limit(50000.0, TimeInForce.GTC),
      quantity = 0.01,
      reduceOnly = false,
      clientOrderId = clientOrderId,
    )

  test("USDT 余额更新，非 USDT 资产不影响"):
    val state = StateManager(List("BTCUSDT"), orderTimeoutMs = 5000)
    state.apply(Event.at(Topics.Balance, Balance(Exchange.Binance, USDT, 1000.0, t0), t0))
    state.apply(Event.at(Topics.Balance, Balance(Exchange.Binance, "BTC", 2.0, t0), t0))
    assertEquals(state.usdtBalance(Exchange.Binance), Some(1000.0))
    assertEquals(state.totalUsdtBalance, 1000.0)

  test("AccountInfo 原子更新 equity 与 notional"):
    val state = StateManager(List("BTCUSDT"), orderTimeoutMs = 5000)
    assertEquals(state.equity(Exchange.Binance), None)
    state.apply(Event.at(Topics.AccountInfo, AccountInfo(Exchange.Binance, 5000.0, 12000.0), t0))
    assertEquals(state.equity(Exchange.Binance), Some(5000.0))
    assertEquals(state.accountNotional(Exchange.Binance), Some(12000.0))
    assertEquals(state.totalEquity, 5000.0)

  test("symbol 事件路由到对应 SymbolState"):
    val state = StateManager(List("BTCUSDT", "ETHUSDT"), orderTimeoutMs = 5000)
    val bbo = BBO(Exchange.Binance, "ETHUSDT", 1600.0, 1.0, 1600.1, 1.0, t0)
    state.apply(Event.at(Topics.Bbo, bbo, t0))
    assertEquals(state.symbolState("ETHUSDT").flatMap(_.bbo(Exchange.Binance)), Some(bbo))
    assertEquals(state.symbolState("BTCUSDT").flatMap(_.bbo(Exchange.Binance)), None)

  test("Clock 事件驱动超时校验: 超时未确认订单 -> 抛错终止"):
    val state = StateManager(List("BTCUSDT"), orderTimeoutMs = 5000)
    state.addPendingOrder(newOrder("BTCUSDT", "c1"), t0) // createdAt = t0
    assert(state.hasPendingOrders("BTCUSDT"))
    // 未超时不抛
    state.apply(Event.stamped(Topics.Clock, (), 0, t0 + 1000))
    // localTs 远超 createdAt + timeout -> 结果不确定，终止
    intercept[RuntimeException] {
      state.apply(Event.stamped(Topics.Clock, (), 0, t0 + 60_000))
    }

  test("向未注册 symbol 下单立即暴露配置错误"):
    val state = StateManager(List("BTCUSDT"), orderTimeoutMs = 5000)
    intercept[RuntimeException] {
      state.addPendingOrder(newOrder("DOGEUSDT", "c1"), t0)
    }

  test("未订阅标的的事件只可能是路由 bug -> 抛错终止"):
    // 归属校验在 StateManager (按 Instrument 定位 SymbolState)，不在 SymbolState:
    // 路由键由载荷派生，事件到了这里 symbol 必然已注册，找不到就是路由坏了。
    val state = StateManager(Set("BTCUSDT"), orderTimeoutMs = 0L)
    val other = BBO(Exchange.Binance, "ETHUSDT", 1.0, 1.0, 2.0, 1.0, t0)
    intercept[RuntimeException] {
      state.apply(Event.at(Topics.Bbo, other, t0))
    }
