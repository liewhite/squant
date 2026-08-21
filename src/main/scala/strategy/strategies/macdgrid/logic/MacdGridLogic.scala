package strategy.strategies.macdgrid.logic

import hft.domain.Side

/** MACD 网格策略**纯决策逻辑** —— 把 (DEA / 柱 / 现价 / MA20 / ATR / 当前份数) 翻译成"该挂的两张单"。
  * 全是纯函数, 无副作用、可直接断言。
  *
  * 角色分工 (1h)：
  *   - **DEA (signal 线) 定持仓方向**：水上 (>0) 只做多、水下 (<0) 只做空；持仓中 DEA 反向穿零 -> **立刻市价全平**。
  *   - **柱 (histogram) 定交易方向**：顺势 (多头看柱水上 / 空头看柱水下) 才**加仓**, 否则**只减仓** (止盈收紧)。
  *   - **MA20 定超买超卖** (现价距 MA20 的 ATR 数)：拉伸过 [[Params.stretchStop]] -> 停止同向加仓;
  *     过 [[Params.stretchTight]] -> 止盈距收紧到 [[Params.tightCloseAtr]]。
  *
  * 挂单结构 (滚动单份括号)：任一时刻至多**一张加仓单 + 一张止盈单, 各一份**, 价格由**锚价** (最近成交价,
  * 冷启动用现价) ± ATR 算出, 上下距离可不同 (加仓 [[Params.addAtr]] / 止盈 [[Params.closeAtr]])。成交后由
  * 调用方用新成交价重算 (本函数只描述"此刻该挂什么")。多空对称。
  */
object MacdGridLogic:

  /** 网格参数 (ATR 倍数 / 最多份数)。常量集中, 改一处即可, 回测可扫。 */
  final case class Params(
      addAtr: Double = 1.0,        // 加仓挂单距锚价 (多: 下方1ATR买 / 空: 上方1ATR卖)
      closeAtr: Double = 2.0,      // 常规止盈距
      tightCloseAtr: Double = 1.0, // 收紧止盈距 (柱反向 或 过度拉伸时)
      stretchStop: Double = 5.0,   // 现价距MA20 > 该ATR数 -> 停止同向加仓
      stretchTight: Double = 8.0,  // 现价距MA20 > 该ATR数 -> 止盈收紧到 tightCloseAtr
      maxUnits: Int = 5,           // 单向最多持有份数
  )

  /** 一张挂单意图：方向、限价、数量(币)、是否只减仓、是否**追价(trailing)**。
    * trail=true 的止盈单价由**现价**算出 (而非锚价), 调用方对其按固定节奏 (如每10秒) 撤单追挂; trail=false 静态 resting。 */
  final case class OrderSpec(side: Side, price: Double, qty: Double, reduceOnly: Boolean, trail: Boolean = false)

  /** 决策：是否市价全平 (DEA 转向), 以及该挂的加仓单 / 止盈单 (各至多一份)。 */
  final case class Decision(flatten: Boolean, add: Option[OrderSpec], close: Option[OrderSpec])

  private val Hold = Decision(flatten = false, add = None, close = None)
  private val Flat = Decision(flatten = true, add = None, close = None)

  /** 纯决策。
    *
    * @param dea        1h MACD DEA (signal) —— 定方向 (水上多/水下空)
    * @param bar        1h MACD 柱 (histogram) —— 定加仓/减仓
    * @param markPrice  现价 (超买超卖判断用: 距 MA20 的 ATR 数)
    * @param levelPrice 挂单锚价 (最近成交价; 冷启动用现价) —— 加仓/止盈价由它 ± ATR 算
    * @param ma20       1h MA20
    * @param atr        1h ATR (<=0 视为未就绪)
    * @param posUnits   当前持仓份数 (有符号: +多 / -空 / 0 平)
    * @param unitQty    一份的币数 (>0)
    */
  def decide(
      dea: Double,
      bar: Double,
      markPrice: Double,
      levelPrice: Double,
      ma20: Double,
      atr: Double,
      posUnits: Int,
      unitQty: Double,
      p: Params,
  ): Decision =
    if atr <= 0.0 || unitQty <= 0.0 then Hold
    else if posUnits > 0 && dea < 0.0 then Flat // 持多 + DEA 转水下 -> 立刻市价平
    else if posUnits < 0 && dea > 0.0 then Flat // 持空 + DEA 转水上 -> 立刻市价平
    else
      val stretchAbove = (markPrice - ma20) / atr // 现价在 MA20 上方几个 ATR (多头超买度)
      val stretchBelow = (ma20 - markPrice) / atr // 现价在 MA20 下方几个 ATR (空头超卖度)

      val add: Option[OrderSpec] =
        if dea > 0.0 && posUnits >= 0 && bar > 0.0 && posUnits < p.maxUnits && stretchAbove <= p.stretchStop then
          Some(OrderSpec(Side.Long, levelPrice - p.addAtr * atr, unitQty, reduceOnly = false))
        else if dea < 0.0 && posUnits <= 0 && bar < 0.0 && -posUnits < p.maxUnits && stretchBelow <= p.stretchStop then
          Some(OrderSpec(Side.Short, levelPrice + p.addAtr * atr, unitQty, reduceOnly = false))
        else None

      // 被动平仓 (单侧挂单: 持仓但加仓被抑制 -> add 为空): 止盈单**追价**, 价用现价算; 否则静态锚价。
      val trailing = add.isEmpty
      val closeRef = if trailing then markPrice else levelPrice
      val close: Option[OrderSpec] =
        if posUnits > 0 then
          val tight = bar < 0.0 || stretchAbove > p.stretchTight
          val d = (if tight then p.tightCloseAtr else p.closeAtr) * atr
          Some(OrderSpec(Side.Short, closeRef + d, unitQty, reduceOnly = true, trail = trailing))
        else if posUnits < 0 then
          val tight = bar > 0.0 || stretchBelow > p.stretchTight
          val d = (if tight then p.tightCloseAtr else p.closeAtr) * atr
          Some(OrderSpec(Side.Long, closeRef - d, unitQty, reduceOnly = true, trail = trailing))
        else None

      Decision(flatten = false, add = add, close = close)
