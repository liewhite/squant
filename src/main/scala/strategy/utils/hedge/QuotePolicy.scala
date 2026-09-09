package strategy.utils.hedge

/** 按**体制**挑选报价方式 —— "现在该慢慢挂还是立刻吃"。
  *
  * 判据是**效率比** ER = |净位移| / |路径长度| ∈ [0,1]：→1 说明敞口在走直线（单边），
  * →0 说明来回折返（震荡）。对冲策略已经在算它了（`EfficiencyRatio`），所以不必再引入第二个
  * 体制指标。注意这个 ER 是**敞口序列**的，而不是价格的：要追的是敞口，行情单边只是它单边的原因。
  */
trait QuotePolicy:
  /** @param efficiencyRatio 效率比 ER（见 `hft.indicator.EfficiencyRatio`）；None = 预热不足 */
  def styleFor(efficiencyRatio: Option[Double]): QuoteStyle

object QuotePolicy:
  /** 恒用一种方式（价格轴对冲策略的既有行为：一直被动挂） */
  def fixed(style: QuoteStyle): QuotePolicy = new QuotePolicy:
    def styleFor(efficiencyRatio: Option[Double]): QuoteStyle = style

  /** ER < `trendThreshold` 用 `calm`，否则用 `trending`。
    *
    * **ER 预热不足时用 `trending`**（更紧迫的那个），这与"读数不可信就不动"的取舍方向相反，
    * 是有意的：那里不动是安全的，这里不动意味着**裸着敞口**。冷启动的头几十分钟本来就按真实
    * 敞口判越界（触发更准也更频繁），若同时用一种可能一直不成交的方式去执行，就成了
    * "判出来了却没冲上" —— 负 gamma 组合最贵的一种失败。宁可多付手续费。
    */
  def byEfficiency(trendThreshold: Double, calm: QuoteStyle, trending: QuoteStyle): QuotePolicy =
    require(trendThreshold > 0.0 && trendThreshold < 1.0, s"ER 阈值须 ∈ (0,1), 实为 $trendThreshold")
    new QuotePolicy:
      def styleFor(efficiencyRatio: Option[Double]): QuoteStyle =
        efficiencyRatio match
          case Some(er) if er < trendThreshold => calm
          case _                               => trending // 含预热不足: 宁可多付手续费也不裸着
