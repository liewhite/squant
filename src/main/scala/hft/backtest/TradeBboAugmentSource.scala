package hft.backtest

import hft.domain.BBO
import hft.event.{AnyEvent, Event, Topics}

/** 在每条 [[Topics.Trade]] **之后追加**一条零价差 [[Topics.Bbo]]
  * (bid=ask=成交价)，其余事件原样透传。
  *
  * 与 [[TradePrintBboSource]] 的区别：后者把 trade **替换**为 BBO (会切断依赖 trade 的下游，如
  * [[BsGreeksSource]] 的希腊字母合成)；本装饰器是**附加**——trade 仍在流中 (greeks/MACD/逐笔撮合
  * 照常)，同时补出 BBO 供**市价单撮合取对手价** ([[hft.sim.SimState.onOrderArrived]] 的 Market
  * 分支需 lastBbo)。零价差是合成近似 (无买卖盘口宽度)，市价成交价即成交价，低估真实点差成本。
  *
  * 用于 trade-native 行情下需要市价 (taker) 对冲的回测；策略不订阅 BBO，BBO 仅供柜台撮合使用。
  */
final class TradeBboAugmentSource(underlying: MarketDataSource) extends MarketDataSource:
  override def events(): Iterator[AnyEvent] =
    underlying.events().flatMap { ev =>
      ev.as(Topics.Trade) match
        case Some(t) =>
          val bbo = Event.stamped(Topics.Bbo, BBO(t.exchange, t.symbol, t.price, t.qty, t.price, t.qty, t.timestamp), ev.exchangeTs, ev.localTs)
          Iterator(ev, bbo)
        case None => Iterator.single(ev)
    }
