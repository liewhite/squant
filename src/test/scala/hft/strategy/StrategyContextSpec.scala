package hft.strategy

import hft.TestUnits.given
import hft.domain.*
import hft.event.Commands.{OrderIntent, OutcomeEvent}
import hft.state.StateManager

class StrategyContextSpec extends munit.FunSuite:
  private val account = AccountId.Live
  private val instrument = Instrument(Exchange.Binance, "BTCUSDT")
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
    StrategyContext(state, account, now = 2L)

  test("只能撤销本策略已登记的挂单"):
    val event = context().cancel(instrument.exchange, instrument.symbol, OrderRef.ByExchangeId(order.id))
    assertEquals(
      event.as(OrderIntent).map(_.outcome),
      Some(OutcomeEvent.CancelOrder(instrument.exchange, instrument.symbol, OrderRef.ByExchangeId(order.id))),
    )

  test("未知订单立即拒绝"):
    val error = intercept[IllegalArgumentException] {
      context().cancel(instrument.exchange, instrument.symbol, OrderRef.ByClientId("not-mine"))
    }
    assert(error.getMessage.contains("只能撤销本策略已登记的挂单"), error.getMessage)

    intercept[IllegalArgumentException] {
      context().cancel(instrument.exchange, instrument.symbol, OrderRef.ByExchangeId(""))
    }

  test("交易所不匹配时拒绝撤单"):
    val error = intercept[IllegalArgumentException] {
      context().cancel(Exchange.Okx, instrument.symbol, OrderRef.ByExchangeId(order.id))
    }
    assert(error.getMessage.contains("exchange=Okx symbol=BTCUSDT"), error.getMessage)

  test("标的不匹配时拒绝撤单"):
    val error = intercept[IllegalArgumentException] {
      context().cancel(instrument.exchange, "ETH", OrderRef.ByExchangeId(order.id))
    }
    assert(error.getMessage.contains("exchange=Binance symbol=ETH"), error.getMessage)
