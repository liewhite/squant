package strategy.utils.option

/** 期权下单量的**精度对齐** —— 两条卖方策略共用的同一份判据。
  *
  * 各写一份的话，改了其中一处不会有任何编译错误 —— 只是从此两条策略在同一个交易所上对
  * "这个量合不合规"给出不同答案，而失效形态是交易所按 lot size 拒单。
  */
object OptionQty:
  /** 按 `step` **向下**对齐并校验 `minQty`：宁少勿多 (不足则下一轮继续补)。
    * 低于最小下单量或对齐后为 0 -> None。
    *
    * `step`/`minQty` 必须为正 —— 从前 `step <= 0` 时**跳过对齐**、`minQty = 0` 放过任何量,
    * 于是交易所报文里一个坏掉的精度字段就能让下单量校验整体失效。两家期权客户端现在都拒绝
    * 精度非正的合约 (见 `OkxOptionsClient.instrumentOf`), 所以这里可以把它写成前置条件。
    *
    * 向下取整前加极小量补偿浮点尾差 —— 否则恰好落在整点上的量 (如算出 1.0 张) 会被 floor
    * 吞成 0。对齐用 BigDecimal 相乘，避免 `steps × step` 自身再引入尾差
    * (`3 × 0.1 = 0.30000000000000004`，交易所会按 lot size 拒单)。
    */
  def alignDown(qty: Double, step: Double, minQty: Double): Option[Double] =
    require(step > 0, s"下单量步长必须为正, 实际 $step —— 校验不了精度的合约不该可交易")
    require(minQty > 0, s"最小下单量必须为正, 实际 $minQty")
    val aligned = (BigDecimal(math.floor(qty / step + 1e-9)) * BigDecimal(step)).toDouble
    if aligned >= minQty && aligned > 0 then Some(aligned) else None
