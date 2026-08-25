package strategy.scan

import hft.domain.{Exchange, Instrument, Side, Symbol, Timestamp}
import hft.event.Topic

/** 一次 taker 流向异动：某标的的单向主动成交，显著强于**它自己的常态**，
  * 且这份强度不是全市场共有的。
  *
  * @param side        异动方向。Long = 主动买爆量，Short = 主动卖爆量
  * @param residualZ   扣掉全市场共同成分后的稳健 z 分（判据本身），带符号
  * @param ownZ        该标的相对自身历史的稳健 z 分
  * @param marketZ     当下全市场 z 分的中位数（被扣掉的共同成分）
  * @param windowFlow  窗口内 taker 净流向（买 − 卖，名义额）
  * @param windowNotional 窗口内双边名义额，用于判断这条信号背后有多少真金白银
  */
final case class FlowAnomaly(
    exchange: Exchange,
    symbol: Symbol,
    side: Side,
    residualZ: Double,
    ownZ: Double,
    marketZ: Double,
    windowFlow: Double,
    windowNotional: Double,
    timestamp: Timestamp,
):
  def instrument: Instrument = Instrument(exchange, symbol)

/** 异动信号通道。
  *
  * **刻意不是 `MarketTopic`**：`MarketTopic` 在本框架里意味着"声明它即表示交易该标的"
  * （见 [[hft.event.Subscription.instruments]]），策略一旦用 `.market` 订阅就会独占该标的。
  * 异动信号是**关注**不是**交易** —— 下游用 `.custom(FlowAnomalies, keys)` 订阅，
  * 读到之后要不要真去交易那个标的，是它自己的决定。
  */
object FlowAnomalies extends Topic[Instrument, FlowAnomaly]("flowAnomaly"):
  def keyOf(payload: FlowAnomaly): Instrument = payload.instrument
