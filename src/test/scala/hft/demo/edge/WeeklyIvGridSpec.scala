package hft.demo.edge

import java.time.LocalDate

/** 周度 IV 网格纯逻辑单测：IV/倍数的因果与**无前视**、网格阈值、种子；7 天窗口切分。 */
class WeeklyIvGridSpec extends munit.FunSuite:
  private val seed = 0.55
  private val step = WeeklyIvGrid.StepGrid(1.5, 0.75)
  private def near(a: Double, b: Double): Unit = assert(math.abs(a - b) < 1e-9, s"expected $b got $a")

  test("首周: IV=种子, StepGrid 倍数=1.0 (不读任何 RV)"):
    val p = WeeklyIvGrid.planWeek(0, seed, _ => fail("不应读取 RV"), step)
    near(p.iv, seed); near(p.ivPrev, seed); near(p.mult, 1.0)

  test("次周: IV=上周RV, ivPrev=种子; StepGrid 波动降→1.5×, 升→0.75×, 平→1.0×"):
    near(WeeklyIvGrid.planWeek(1, seed, _ => 0.50, step).mult, 1.5) // rv(0)=0.50<0.55 降
    near(WeeklyIvGrid.planWeek(1, seed, _ => 0.60, step).mult, 0.75) // 0.60>0.55 升
    near(WeeklyIvGrid.planWeek(1, seed, _ => 0.55, step).mult, 1.0)  // 持平
    val p = WeeklyIvGrid.planWeek(1, seed, _ => 0.50, step)
    near(p.iv, 0.50); near(p.ivPrev, seed)

  test("第3周起: IV=上周RV, ivPrev=上上周RV"):
    val rv = Map(0 -> 0.80, 1 -> 0.60) // idx2: iv=rv(1)=0.60, ivPrev=rv(0)=0.80 -> 降 -> 1.5×
    val p = WeeklyIvGrid.planWeek(2, seed, rv, step)
    near(p.iv, 0.60); near(p.ivPrev, 0.80); near(p.mult, 1.5)

  test("无前视: planWeek(idx) 只读 rv(j) 且 j<=idx-1"):
    // rv 查询若被以未来索引调用则 fail；planWeek 不应触发
    def guarded(idx: Int): Int => Double = j =>
      assert(j <= idx - 1, s"前视! 第 $idx 周读取了 rv($j)")
      0.5 + j * 0.01
    (0 to 8).foreach(i => WeeklyIvGrid.planWeek(i, seed, guarded(i), step))

  test("DropOnly: 只在波动下降买, 越跌越买 (按相对跌幅), 上升/首周→0, 受 cap 钳制"):
    val d = WeeklyIvGrid.DropOnly(scale = 5.0, cap = 3.0)
    near(d.mult(0.60, 0.80, 2), 1.25)           // 跌 25% -> 5*0.25=1.25
    near(d.mult(0.40, 0.80, 2), 2.5)            // 跌 50% -> 5*0.5=2.5 (<cap)
    near(d.mult(0.10, 0.80, 2), 3.0)            // 跌 87.5% -> 5*0.875=4.375 -> cap 3.0
    near(d.mult(0.80, 0.60, 2), 0.0)            // 上升 -> 0
    near(d.mult(0.60, 0.60, 2), 0.0)            // 持平 -> 0
    near(d.mult(0.55, 0.55, 0), 0.0)            // 首周 -> 0

  // ---- weekWindows ----
  private def d(n: Int): LocalDate = LocalDate.of(2025, 1, 1).plusDays(n)

  test("连续 14 天 -> 2 个不重叠窗口"):
    val ws = WeeklyIvGrid.weekWindows((0 to 13).map(d).toSet)
    assertEquals(ws, Seq((d(0), d(6)), (d(7), d(13))))

  test("中间缺天 -> 含缺天窗口整窗跳过 (步长不回退)"):
    // 0..6 全, 7..13 缺 10, 14..20 全 -> 只剩 [0-6],[14-20]
    val dates = ((0 to 6) ++ (7 to 13).filter(_ != 10) ++ (14 to 20)).map(d).toSet
    assertEquals(WeeklyIvGrid.weekWindows(dates), Seq((d(0), d(6)), (d(14), d(20))))

  test("不足 7 天 / 空集 -> 无窗口"):
    assertEquals(WeeklyIvGrid.weekWindows((0 to 5).map(d).toSet), Seq.empty)
    assertEquals(WeeklyIvGrid.weekWindows(Set.empty), Seq.empty)

  test("windows: 21 天窗口、7 天步长 -> 重叠"):
    // 0..27 全 (28 天) -> 起点 0,7,14 (21 天窗各需起点+20<=27); 起点21 -> 21+20=41>27 跳过
    val ws = WeeklyIvGrid.windows((0 to 27).map(d).toSet, lenDays = 21, stepDays = 7)
    assertEquals(ws, Seq((d(0), d(20)), (d(7), d(27))))
    assertEquals(WeeklyIvGrid.windows((0 to 13).map(d).toSet, 7, 7), WeeklyIvGrid.weekWindows((0 to 13).map(d).toSet))
