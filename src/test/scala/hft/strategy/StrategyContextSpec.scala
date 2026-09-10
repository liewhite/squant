package hft.strategy

import hft.TestUnits.given
import hft.domain.*
import hft.event.Commands.{OrderIntent, OutcomeEvent}
import hft.state.StateManager

class StrategyContextSpec extends munit.FunSuite:
  private val account = AccountId.Live
  private val instrument = Instrument.perp(Exchange.Binance, "BTCUSDT")
  private val order = Order(
    id = "exchange-1",
    exchange = instrument.exchange,
    symbol = instrument.symbol,
    side = Side.Long,
    orderType = OrderType.Limit(50_000.0, TimeInForce.GTC),
    quantity = Coin(0.01),
    reduceOnly = false,
    clientOrderId = "client-1",
  )

  private def context(): StrategyContext =
    val state = StateManager(List(instrument), orderTimeoutMs = 0L)
    state.addPendingOrder(order, now = 1L)
    StrategyContext(state, account, now = 2L, orderTag = None)

  test("只能撤销本策略已登记的挂单"):
    val event = context().cancel(instrument, OrderRef.ByExchangeId(order.id))
    assertEquals(
      event.as(OrderIntent).map(_.outcome),
      Some(OutcomeEvent.CancelOrder(instrument.exchange, instrument.symbol, OrderRef.ByExchangeId(order.id))),
    )

  test("未知订单立即拒绝"):
    val error = intercept[IllegalArgumentException] {
      context().cancel(instrument, OrderRef.ByClientId("not-mine"))
    }
    assert(error.getMessage.contains("只能撤销本策略已登记的挂单"), error.getMessage)

    intercept[IllegalArgumentException] {
      context().cancel(instrument, OrderRef.ByExchangeId(""))
    }

  test("交易所不匹配时拒绝撤单"):
    val error = intercept[IllegalArgumentException] {
      context().cancel(Instrument.perp(Exchange.Okx, instrument.symbol), OrderRef.ByExchangeId(order.id))
    }
    assert(error.getMessage.contains("instrument=Okx:BTCUSDT"), error.getMessage)

  test("标的不匹配时拒绝撤单"):
    val error = intercept[IllegalArgumentException] {
      context().cancel(Instrument.perp(instrument.exchange, "ETH"), OrderRef.ByExchangeId(order.id))
    }
    assert(error.getMessage.contains("instrument=Binance:ETH"), error.getMessage)
