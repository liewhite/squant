package hft.backtest

import hft.domain.BBO
import hft.event.{AnyEvent, Event, Topics}

/** 用逐笔成交合成 L1 盘口的数据源装饰器 —— **真实 BBO 缺位时的显式补位**。
  *
  * 背景：币安 futures 的每日 bookTicker 文件止于 2024-03-30，此后区间只有 trades。而
  * taker 单要成交必须有对手价 (见 [[hft.sim.SimState.onOrderArrived]] 取 [[hft.sim.Matcher.touchPrice]])，
  * 依赖盘口的策略也收不到行情。
  *
  * 做法：在每条 [[Topics.Trade]] **之后追加**一条零价差 [[Topics.Bbo]] (bid = ask = 成交价)，
  * 其余事件原样透传。追加而非替换 —— trade 仍在流中，逐笔穿越撮合、指标、
  * [[BsGreeksSource]] 的希腊字母合成都照常工作。
  *
  * **它是近似，且偏乐观**：spread = 0 意味着 taker 不付点差，成本被系统性低估 (真实点差在
  * 大流动性标的约 1 个 tick，小市值合约可达数十 bp)。因此：
  *   - 只在**没有真实 BBO 数据**时使用，且由装配方显式套上，绝不静默兜底；
  *   - 有真实 bookTicker 的区间 (2024-03-30 及以前) 直接用 `MarketDataKind.Bbo`，不要套本装饰器。
  *
  * 上游若已带真实 BBO 则**立即抛错**而非静默产出双重盘口 —— 两个 L1 交替喂进撮合会悄悄改变
  * 成交结果，是那种跑完才发现数字不对、还查不出为什么的错。约束写在代码里才算数。
  */
final class SyntheticBboSource(underlying: MarketDataSource) extends MarketDataSource:
  override def events(): Iterator[AnyEvent] =
    underlying.events().flatMap { ev =>
      if ev.is(Topics.Bbo) then
        throw IllegalStateException(
          "SyntheticBboSource wraps a source that already carries real BBO; " +
            "drop the decorator (or drop MarketDataKind.Bbo) instead of mixing real and synthetic L1"
        )
      ev.as(Topics.Trade) match
        case Some(t) =>
          val bbo = BBO(t.exchange, t.symbol, t.price, t.qty, t.price, t.qty, t.timestamp)
          Iterator(ev, Event.stamped(Topics.Bbo, bbo, ev.exchangeTs, ev.localTs))
        case None => Iterator.single(ev)
    }
