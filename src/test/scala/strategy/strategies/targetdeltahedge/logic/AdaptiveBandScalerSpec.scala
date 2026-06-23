package strategy.strategies.targetdeltahedge.logic

import hft.strategy.{OutcomeEvent, Strategy}

import hft.domain.Side

/** 方向自适应带乘子单测：对冲收窄 / 静默回升 / floor 与 cap / 买卖独立。 */
class AdaptiveBandScalerSpec extends munit.FunSuite:
  private val minMs = 60_000L
  private def near(a: Double, b: Double): Unit = assert(math.abs(a - b) < 1e-9, s"expected $b got $a")

  test("首次 (无对冲史) 乘子为 1"):
    val s = AdaptiveBandScaler()
    near(s.mult(Side.Long, 1000), 1.0)
    near(s.mult(Side.Short, 1000), 1.0)

  test("对冲后收窄 10% (×0.9)"):
    val s = AdaptiveBandScaler(floor = 0.5, shrinkPerHedge = 0.10, recoverPerMin = 0.10)
    s.onHedge(Side.Long, 0)
    near(s.mult(Side.Long, 0), 0.9) // 同一时刻无回升

  test("连续对冲收到 floor 即止 (最小半带)"):
    val s = AdaptiveBandScaler(floor = 0.5, shrinkPerHedge = 0.10, recoverPerMin = 0.10)
    for _ <- 1 to 50 do s.onHedge(Side.Long, 0) // 同一时刻多次, 无回升
    near(s.mult(Side.Long, 0), 0.5)

  test("静默每分钟回升 10% (×1.1), 上限 1"):
    val s = AdaptiveBandScaler(floor = 0.5, shrinkPerHedge = 0.50, recoverPerMin = 0.10)
    s.onHedge(Side.Long, 0) // -> 0.5
    near(s.mult(Side.Long, 0), 0.5)
    near(s.mult(Side.Long, minMs), 0.5 * 1.1) // 1 分钟后
    near(s.mult(Side.Long, 2 * minMs), 0.5 * 1.1 * 1.1) // 2 分钟后
    near(s.mult(Side.Long, 100 * minMs), 1.0) // 足够久 -> 封顶 1

  test("买卖方向独立"):
    val s = AdaptiveBandScaler(floor = 0.5, shrinkPerHedge = 0.10, recoverPerMin = 0.10)
    s.onHedge(Side.Long, 0)
    near(s.mult(Side.Long, 0), 0.9)
    near(s.mult(Side.Short, 0), 1.0) // 卖方未受影响
