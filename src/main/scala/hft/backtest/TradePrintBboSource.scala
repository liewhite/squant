package hft.backtest

import hft.domain.BBO
import hft.event.{AnyEvent, Event, Topics}

/** 把成交印记 (trades) 还原为合成 L1 行情的数据源装饰器。
  *
  * 背景：币安 Vision 自 2024 年中起不再发布 futures 每日 bookTicker，较新上市的合约 (如 SIRENUSDT)
  * 根本没有 L1 历史，只有 trades。而撮合核心 [[hft.sim.SimState.onMarket]] 只在 BBO 上撮合 ——
  * 没有 BBO 就没有任何成交。
  *
  * 本装饰器把上游每条 [[Topics.Trade]] 映射为**零价差**的 [[Topics.Bbo]]
  * (bid=ask=成交价)，其余事件原样透传。语义即标准的"成交印记撮合模型"：
  *   - 挂在限价 L 的买单，当有真实成交打到 price<=L 时成交 (价格下穿 L)，maker 成交价取 L；
  *   - 挂在限价 L 的卖单，当有真实成交打到 price>=L 时成交 (价格上穿 L)，maker 成交价取 L。
  *
  * 即"我的挂单会不会被一笔真实成交击穿"。零价差是近似 (无买卖盘口宽度)，但对 maker 策略的
  * 成交判定是合理且偏保守的下界模型。中间价 = 成交价，策略的暴跌因子也据此计算。
  *
  * 假定上游**没有真实 bookTicker**：真实 BBO 原样透传，若来源同时含真实 book + trades 会产生
  * 双重/冲突 L1 (本装饰器只用于无 bookTicker 的较新合约)。
  *
  * **可选/旁路功能** (非默认)：仅当策略写死依赖 BBO、而行情只有 trades 时才启用 (回测引擎的
  * `synthesizeBbo` 开关)。它把 trade **替换**为零价差 BBO —— spread=0 是合成近似，会高估
  * maker 成交。默认回测路径**不启用**，撮合直接用真实 trade ([[hft.sim.SimState.matchTrade]])。
  */
final class TradePrintBboSource(underlying: MarketDataSource) extends MarketDataSource:
  override def events(): Iterator[AnyEvent] =
    underlying.events().map { ev =>
      ev.as(Topics.Trade) match
        case Some(t) =>
          Event.stamped(Topics.Bbo, BBO(t.exchange, t.symbol, t.price, t.qty, t.price, t.qty, t.timestamp), ev.exchangeTs, ev.localTs)
        case None => ev
    }
