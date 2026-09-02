package strategy.strategies.crossspread.logic

import hft.domain.{Instrument, Price, Timestamp}

/** 统一的标的代码 —— 跨所配对的桥。
  *
  * 各所的合约命名互不相同 (币安 `AAPLUSDT`、OKX `AAPL`、Hyperliquid `xyz:AAPL` 落到框架后是
  * `AAPL`)，框架的 `Symbol` 只保证在**本所内**唯一。"这两个合约是同一个东西"因此不是框架
  * 能推出的事实，必须显式建立 —— 这个类型就是那份显式。
  */
type Ticker = String

/** 一条报价 —— 价差计算的全部输入。
  *
  * 用中间价而不是成交价：成交是稀疏且单边的，两所的最近一笔成交可能相隔很久、方向相反，
  * 拿它们相减得到的"价差"里混着买卖价差与时间差。中间价则在两边都是同一时刻的同一口径。
  */
final case class VenueQuote(bid: Price, ask: Price, timestamp: Timestamp):
  def mid: Price = Price.mid(bid, ask)

/** 一个跨所价差对：同一个 [[Ticker]] 在两个交易所上的合约。
  *
  * `a`/`b` 的顺序由构造时固定 (按 `Instrument` 的字符串序)，于是价差的符号有确定含义：
  * 正 = `a` 比 `b` 贵。顺序若随调用方变化，同一对在不同时刻的均线就不是同一个量。
  */
final case class VenuePair(ticker: Ticker, a: Instrument, b: Instrument):
  require(a.exchange != b.exchange, s"价差对必须跨交易所: $a / $b")
  require(a.toString < b.toString, s"价差对的两腿必须按字符串序排列, 请用 VenuePair.of 构造: $a / $b")

  override def toString: String = s"$ticker[${a.exchange}|${b.exchange}]"

object VenuePair:
  /** 按固定顺序构造，调用方不必记得排序 */
  def of(ticker: Ticker, x: Instrument, y: Instrument): VenuePair =
    if x.toString < y.toString then VenuePair(ticker, x, y) else VenuePair(ticker, y, x)
