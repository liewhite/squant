package strategy.scan

import hft.domain.{Side, Symbol}

/** 某个标的在当前窗口的流向读数 —— [[AnomalyRule]] 的**全部**输入。
  *
  * 刻意只是一份数据快照，不是 [[SymbolTakerFlow]] 本身：规则不该知道读数是怎么算出来的
  * （分桶？EWMA？），窗口也不该知道谁在拿它做判定。两边各换各的，互不牵动。
  *
  * @param ownZ 该标的当前窗口净流向相对**它自己**历史的稳健 z 分（纵向归一，已消掉体量差异）
  */
final case class FlowObservation(
    symbol: Symbol,
    ownZ: Double,
    windowFlow: Double,
    windowNotional: Double,
)

/** 判定结论：这个标的异动了，方向与强度如是。 */
final case class FlowVerdict(
    symbol: Symbol,
    side: Side,
    residualZ: Double,
    ownZ: Double,
    marketZ: Double,
)

/** 横截面异动判定规则 —— 本模块唯一的**可替换件**。
  *
  * 输入是当下全市场的读数，输出是"谁异动了"。纯函数：同一批读数必得同一批结论，
  * 不持有状态、不看时钟、不碰框架。换一种判法 = 新增一个实现，驱动层与窗口层一行不动。
  */
trait AnomalyRule:
  def detect(observations: Vector[FlowObservation]): Vector[FlowVerdict]

/** 默认规则：**自身 z 分减去全市场 z 的中位数**。
  *
  * 两级归一里的横向那一级。`ownZ` 已经消掉了标的之间的体量差异（否则榜单永远是 BTC/ETH），
  * 这里再扣掉当下的共同成分 —— 不扣的话大盘一根长阴就会把几百个标的同时报成"卖爆"，
  * 而那恰恰**不是**"独立于其他标的"的异动。
  *
  * 取中位数而非均值：我们要找的少数尖峰不该影响它们被减去的那个基准。均值会被尖峰自己抬高，
  * 于是越异常越检测不出。
  *
  * @param residualZ         触发阈值（扣掉共同成分后的 |z|）
  * @param minWindowNotional 窗口双边名义额下限。枯水标的几笔小单就能把自身尺度撑爆，
  *                          z 分再高也不可信 —— 这条防的是"报一堆没人交易的币"
  * @param minSymbols        少于这么多标的有读数时一律不判：中位数没有意义，
  *                          拿三五个样本当"全市场"比不判更糟
  */
final case class CrossSectionalMedianRule(
    residualZ: Double = 4.0,
    minWindowNotional: Double = 50_000.0,
    minSymbols: Int = 20,
) extends AnomalyRule:

  override def detect(observations: Vector[FlowObservation]): Vector[FlowVerdict] =
    if observations.sizeIs < minSymbols then Vector.empty
    else
      val marketZ = TakerFlowStats.median(observations.map(_.ownZ).toArray)
      observations.flatMap { o =>
        val residual = o.ownZ - marketZ
        Option.when(math.abs(residual) >= residualZ && o.windowNotional >= minWindowNotional) {
          FlowVerdict(o.symbol, if residual > 0 then Side.Long else Side.Short, residual, o.ownZ, marketZ)
        }
      }
