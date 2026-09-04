package hft

import hft.sim.SimConfig

/** 测试专用的仿真配置。
  *
  * [[SimConfig]] 的两个费率**没有默认值** —— 手续费常常就是回测盈亏的量级, 默认 0 会让
  * 每个忘了填的调用方拿到一份偏乐观且无症状的结果 (见 SimConfig 的说明)。
  *
  * 但大量撮合用例要验证的是"什么时候成交、成交价是多少", 与费率无关; 让它们各自写一遍
  * `makerFeeRate = 0.0, takerFeeRate = 0.0` 只是噪音, 还会让"这个用例是**特意**不计费"
  * 与"这个用例忘了填"看起来一模一样。所以给它一个有名字的零费率配置：
  * 用它就是在说"本用例特意不计手续费"。
  *
  * 验证费率本身的用例 (如 `SimStateFeeSpec`) 照旧显式写费率。
  */
object TestSim:
  /** 零费率 —— 用于**与手续费无关**的撮合/时序用例。改延迟/初始资金用 `.copy(...)`。 */
  val noFees: SimConfig = SimConfig(makerFeeRate = 0.0, takerFeeRate = 0.0)
