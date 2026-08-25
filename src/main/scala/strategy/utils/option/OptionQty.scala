package strategy.utils.option

/** 期权下单量的**精度对齐** —— 两条卖方策略共用的同一份判据。
  *
  * 各写一份的话，改了其中一处不会有任何编译错误 —— 只是从此两条策略在同一个交易所上对
  * "这个量合不合规"给出不同答案，而失效形态是交易所按 lot size 拒单。
  */
object OptionQty:
  /** 按 `step` **向下**对齐并校验 `minQty`：宁少勿多 (不足则下一轮继续补)。
    * 低于最小下单量或对齐后为 0 -> None。`step <= 0` 时只校验 minQty。
    *
    * 向下取整前加极小量补偿浮点尾差 —— 否则恰好落在整点上的量 (如算出 1.0 张) 会被 floor
    * 吞成 0。对齐用 BigDecimal 相乘，避免 `steps × step` 自身再引入尾差
    * (`3 × 0.1 = 0.30000000000000004`，交易所会按 lot size 拒单)。
    */
  def alignDown(qty: Double, step: Double, minQty: Double): Option[Double] =
    val aligned =
      if step > 0 then (BigDecimal(math.floor(qty / step + 1e-9)) * BigDecimal(step)).toDouble
      else qty
    if aligned >= minQty && aligned > 0 then Some(aligned) else None
