package hft.state

import hft.domain.*
import hft.state.{StateManager, SymbolState}
import hft.event.{Event, Topics}
import hft.TestUnits.given

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
    val state = StateManager(List(Instrument(Exchange.Binance, "BTCUSDT")), orderTimeoutMs = 5000)
    state.apply(Event.at(Topics.Balance, Balance(AccountId.Live, Exchange.Binance, USDT, 1000.0, t0), t0))
    state.apply(Event.at(Topics.Balance, Balance(AccountId.Live, Exchange.Binance, "BTC", 2.0, t0), t0))
    assertEquals(state.usdtBalance(Exchange.Binance), Some(1000.0))
    assertEquals(state.totalUsdtBalance, 1000.0)

  test("AccountInfo 按交易所更新净值"):
    // 名义价值那个字段已经删掉了: 三家里两家的 REST 填 0, 而主代码里没有人读它 ——
    // 见 AccountInfo 的说明。要杠杆率由读得到持仓的一方自己算。
    val state = StateManager(List(Instrument(Exchange.Binance, "BTCUSDT")), orderTimeoutMs = 5000)
    assertEquals(state.equity(Exchange.Binance), None)
    state.apply(Event.at(Topics.AccountInfo, AccountInfo(AccountId.Live, Exchange.Binance, 5000.0), t0))
    assertEquals(state.equity(Exchange.Binance), Some(5000.0))
    assertEquals(state.totalEquity, 5000.0)

  test("symbol 事件路由到对应 SymbolState"):
    val state = StateManager(List(Instrument(Exchange.Binance, "BTCUSDT"), Instrument(Exchange.Binance, "ETHUSDT")), orderTimeoutMs = 5000)
    val bbo = BBO(Exchange.Binance, "ETHUSDT", 1600.0, Coin(1.0), 1600.1, Coin(1.0), t0)
    state.apply(Event.at(Topics.Bbo, bbo, t0))
    assertEquals(state.symbolState("ETHUSDT").flatMap(_.bbo(Exchange.Binance)), Some(bbo))
    assertEquals(state.symbolState("BTCUSDT").flatMap(_.bbo(Exchange.Binance)), None)

  test("Clock 事件驱动超时校验: 超时未确认订单 -> 抛错终止"):
    val state = StateManager(List(Instrument(Exchange.Binance, "BTCUSDT")), orderTimeoutMs = 5000)
    state.addPendingOrder(newOrder("BTCUSDT", "c1"), t0) // createdAt = t0
    assert(state.hasPendingOrders("BTCUSDT"))
    // 未超时不抛
    state.apply(Event.stamped(Topics.Clock, (), 0, t0 + 1000))
    // localTs 远超 createdAt + timeout -> 结果不确定，终止
    intercept[RuntimeException] {
      state.apply(Event.stamped(Topics.Clock, (), 0, t0 + 60_000))
    }

  test("向未注册 symbol 下单立即暴露配置错误"):
    val state = StateManager(List(Instrument(Exchange.Binance, "BTCUSDT")), orderTimeoutMs = 5000)
    intercept[RuntimeException] {
      state.addPendingOrder(newOrder("DOGEUSDT", "c1"), t0)
    }

  test("未订阅标的的事件只可能是路由 bug -> 抛错终止"):
    // 归属校验在 StateManager (按 Instrument 定位 SymbolState)，不在 SymbolState:
    // 路由键由载荷派生，事件到了这里 symbol 必然已注册，找不到就是路由坏了。
    val state = StateManager(Set(Instrument(Exchange.Binance, "BTCUSDT")), orderTimeoutMs = 0L)
    val other = BBO(Exchange.Binance, "ETHUSDT", 1.0, Coin(1.0), 2.0, Coin(1.0), t0)
    intercept[RuntimeException] {
      state.apply(Event.at(Topics.Bbo, other, t0))
    }

  test("私有回报按 AccountInstrument 路由, 仍要落到对应的 SymbolState"):
    // 回归防线: 私有回报的 key 从 Instrument 换成 AccountInstrument 时, 这里的定位逻辑
    // 一度没跟上 —— `case _: Instrument` 对新 key 永不匹配, 于是成交被静默忽略、
    // 仓位不更新、挂单拿不到交易所 id。300+ 个测试里只有一个间接路径抓到了它。
    val state = StateManager(Set(Instrument(Exchange.Binance, "BTCUSDT")), orderTimeoutMs = 0L)
    val ex = Exchange.Binance

    state.apply(Event.local(Topics.Position, Position(AccountId.Live, ex, "BTCUSDT", Coin(2.0))))
    assertEqualsDouble(state.symbolState("BTCUSDT").get.positionSize(ex).value, 2.0, 1e-12, "仓位快照必须落到 SymbolState")

    val order = Order("", ex, "BTCUSDT", Side.Long, OrderType.Limit(99.0, TimeInForce.GTC), 1.0, reduceOnly = false, clientOrderId = "c1")
    state.addPendingOrder(order, t0)
    state.apply(Event.local(
      Topics.OrderUpdate,
      OrderUpdate(AccountId.Live, "EX-9", Some("c1"), ex, "BTCUSDT", Side.Long, OrderStatus.Pending, 99.0, Coin(1.0), Coin(0.0), reduceOnly = false, t0),
    ))
    assertEquals(
      state.symbolState("BTCUSDT").get.pendingOrders.head.order.id,
      "EX-9",
      "OrderUpdate 必须回填交易所 id —— 否则停机收尾撤不掉这张单",
    )

  test("行情仍按 Instrument 路由 (无账户维度), 同样要落到 SymbolState"):
    val state = StateManager(Set(Instrument(Exchange.Binance, "BTCUSDT")), orderTimeoutMs = 0L)
    val ex = Exchange.Binance
    state.apply(Event.at(Topics.Bbo, BBO(ex, "BTCUSDT", 100.0, Coin(1.0), 100.1, Coin(1.0), t0), t0))
    assertEquals(state.symbolState("BTCUSDT").flatMap(_.bbo(ex)).map(_.bidPrice.value), Some(100.0))

  test("钱包快照到达前, greeks 返回 None —— 而不是把'没见过该币'当成 0"):
    // 期权 delta 对冲要 `期权 delta + 该币现金余额` 才是总敞口。缺余额只能暂停对冲,
    // 不能按残缺敞口下单。从前有个 feed 注入假的 BalanceChanged(ccy, 0.0) 绕过这一点。
    val state = StateManager(List(Instrument(Exchange.Okx, "ETH-USDT-SWAP")), orderTimeoutMs = 5000)
    state.apply(Event.local(Topics.Greeks, Greeks(AccountId.Live, Exchange.Okx, "ETH", 1.0, 0.1, 0.0, 0.0, t0)))
    assertEquals(state.greeks(Exchange.Okx, "ETH"), None, "没有钱包快照 -> 给不出总敞口")

  test("钱包快照之后, 未列出的币种余额如实解读为 0"):
    // OKX 余额为 0 时不下发该币种行 —— 只卖期权不持现货的账户永远等不到那条 Balance。
    // "完整"这个事实由协议给出 (REST 钱包一次返回整份), 因此缺失即可解读为 0。
    val state = StateManager(List(Instrument(Exchange.Okx, "ETH-USDT-SWAP")), orderTimeoutMs = 5000)
    state.apply(Event.local(Topics.Greeks, Greeks(AccountId.Live, Exchange.Okx, "ETH", 1.0, 0.1, 0.0, 0.0, t0)))
    state.apply(Event.local(Topics.Wallet, Wallet(AccountId.Live, Exchange.Okx, Map("USDT" -> 500.0), t0)))
    assertEquals(state.greeks(Exchange.Okx, "ETH").map(_.delta), Some(1.0), "ETH 未列出 -> 现货修正为 0")

  test("钱包快照整表替换 —— 清空的现货不再参与 delta 修正"):
    val state = StateManager(List(Instrument(Exchange.Okx, "ETH-USDT-SWAP")), orderTimeoutMs = 5000)
    state.apply(Event.local(Topics.Greeks, Greeks(AccountId.Live, Exchange.Okx, "ETH", 1.0, 0.1, 0.0, 0.0, t0)))
    state.apply(Event.local(Topics.Wallet, Wallet(AccountId.Live, Exchange.Okx, Map("ETH" -> 2.0), t0)))
    assertEquals(state.greeks(Exchange.Okx, "ETH").map(_.delta), Some(3.0), "1.0 + 2.0 现货")
    // 下一份快照里 ETH 没了 = 现货已清空
    state.apply(Event.local(Topics.Wallet, Wallet(AccountId.Live, Exchange.Okx, Map("USDT" -> 500.0), t0)))
    assertEquals(state.greeks(Exchange.Okx, "ETH").map(_.delta), Some(1.0), "留着旧值会让已清空的现货继续参与修正")

  test("钱包快照只清本交易所 —— 不抹掉另一家的余额"):
    val state = StateManager(
      List(Instrument(Exchange.Okx, "ETH-USDT-SWAP"), Instrument(Exchange.Bybit, "ETHUSDT")),
      orderTimeoutMs = 5000,
    )
    state.apply(Event.local(Topics.Greeks, Greeks(AccountId.Live, Exchange.Okx, "ETH", 1.0, 0.1, 0.0, 0.0, t0)))
    state.apply(Event.local(Topics.Greeks, Greeks(AccountId.Live, Exchange.Bybit, "ETH", 1.0, 0.1, 0.0, 0.0, t0)))
    state.apply(Event.local(Topics.Wallet, Wallet(AccountId.Live, Exchange.Bybit, Map("ETH" -> 3.0), t0)))
    state.apply(Event.local(Topics.Wallet, Wallet(AccountId.Live, Exchange.Okx, Map("USDT" -> 1.0), t0)))
    assertEquals(state.greeks(Exchange.Bybit, "ETH").map(_.delta), Some(4.0), "OKX 的快照不该动 Bybit 的现货")
    assertEquals(state.greeks(Exchange.Okx, "ETH").map(_.delta), Some(1.0))

  test("钱包快照里没有 USDT = 计价货币余额确实是 0"):
    // 从前这里是 `foreach`: 快照没有 USDT 时留着旧值, 正是这份快照要消灭的
    // "已清空却继续参与计算"。
    val state = StateManager(List(Instrument(Exchange.Okx, "ETH-USDT-SWAP")), orderTimeoutMs = 5000)
    state.apply(Event.local(Topics.Wallet, Wallet(AccountId.Live, Exchange.Okx, Map("USDT" -> 500.0), t0)))
    assertEquals(state.usdtBalance(Exchange.Okx), Some(500.0))
    state.apply(Event.local(Topics.Wallet, Wallet(AccountId.Live, Exchange.Okx, Map("ETH" -> 2.0), t0)))
    assertEquals(state.usdtBalance(Exchange.Okx), Some(0.0), "留着旧的 500 就是拿一笔已清空的现金继续做决策")

  test("WS 的逐币种余额在快照之后维持它 —— 只改这一个币种, 不动其它"):
    // 三家 WS 推的都是"该币种当前余额", 只是不覆盖未变动的币种; 所以增量维护是正确的。
    val state = StateManager(List(Instrument(Exchange.Okx, "ETH-USDT-SWAP")), orderTimeoutMs = 5000)
    state.apply(Event.local(Topics.Greeks, Greeks(AccountId.Live, Exchange.Okx, "ETH", 1.0, 0.1, 0.0, 0.0, t0)))
    state.apply(Event.local(Topics.Wallet, Wallet(AccountId.Live, Exchange.Okx, Map("USDT" -> 500.0, "ETH" -> 2.0), t0)))
    state.apply(Event.local(Topics.Balance, Balance(AccountId.Live, Exchange.Okx, "USDT", 600.0, t0)))
    assertEquals(state.greeks(Exchange.Okx, "ETH").map(_.delta), Some(3.0), "只有 USDT 变动, ETH 现货必须还在")
    state.apply(Event.local(Topics.Balance, Balance(AccountId.Live, Exchange.Okx, "ETH", 0.0, t0)))
    assertEquals(state.greeks(Exchange.Okx, "ETH").map(_.delta), Some(1.0), "ETH 归零如实生效")
