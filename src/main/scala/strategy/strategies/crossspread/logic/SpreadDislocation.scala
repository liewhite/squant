package strategy.strategies.crossspread.logic

import hft.domain.{Instrument, Price, Timestamp}
import hft.event.Topic

/** 一次跨所价差异动：同一标的在两所之间的价差，**突然**偏离了它自己近期的中枢。
  *
  * 只有偏离才是信号，价差本身不是：两所之间长期存在的价差是资金费率、上市时间、
  * 参与者结构等结构性因素的结果，它可以稳稳地挂在 30bp 上好几天。照着它开仓，
  * 等来的不是回归而是持仓成本。
  *
  * 方向已经由 [[rich]]/[[cheap]] 表达，故 [[deviationBps]] 恒为正：贵的那一边是异动中
  * 被推高的一边，回归意味着卖 [[rich]]、买 [[cheap]]。
  *
  * @param spreadBps    当前价差 = 1e4 × ln(rich 中间价 / cheap 中间价)
  * @param meanBps      同方向的窗口均线 (价差中枢)
  * @param sigmaBps     窗口内价差的标准差
  * @param deviationBps 当前价差减去均线 —— 判据本身
  * @param z            偏离 / max(σ, 规则下限)
  * @param crossEdgeBps 此刻**对敲吃单**能拿到的原始价差: 1e4 × ln(rich 买一 / cheap 卖一)。
  *                     它不是利润 —— 利润是它相对中枢的部分再扣手续费 —— 但它是负数时,
  *                     连这次异动的方向都吃不动
  * @param quoteAgeMs   两腿报价中较旧那条的年龄，用来判断这条信号背后有没有一边其实停更了
  */
final case class SpreadDislocation(
    ticker: Ticker,
    rich: Instrument,
    cheap: Instrument,
    spreadBps: Double,
    meanBps: Double,
    sigmaBps: Double,
    deviationBps: Double,
    z: Double,
    crossEdgeBps: Double,
    richMid: Price,
    cheapMid: Price,
    samples: Int,
    quoteAgeMs: Long,
    timestamp: Timestamp,
)

/** 价差异动信号通道。
  *
  * **刻意不是 `MarketTopic`**：在本框架里用 `MarketTopic` 声明一个标的即表示**交易**它
  * (框架据此补齐私有回报、做启动对齐、独占登记)。异动信号是**关注**不是交易 ——
  * 下游用 `.custom(SpreadDislocations, keys)` 订阅，要不要据此下单是它自己的决定。
  */
object SpreadDislocations extends Topic[Ticker, SpreadDislocation]("spreadDislocation"):
  def keyOf(payload: SpreadDislocation): Ticker = payload.ticker

object SpreadDislocation:
  /** 人读的一行 —— 驱动与启动器共用，免得两处各拼一遍格式 */
  def describe(d: SpreadDislocation): String =
    f"[价差异动] ${d.ticker}%-6s ${d.rich.exchange}%-11s 贵于 ${d.cheap.exchange}%-11s " +
      f"偏离=${d.deviationBps}%7.1fbp (现价差 ${d.spreadBps}%7.1f 中枢 ${d.meanBps}%7.1f σ=${d.sigmaBps}%5.1f z=${d.z}%5.1f) " +
      f"对敲价差=${d.crossEdgeBps}%7.1fbp 报价 ${d.richMid.value}%.4f/${d.cheapMid.value}%.4f " +
      f"样本=${d.samples} 报价延迟=${d.quoteAgeMs}ms"
