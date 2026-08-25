package hft.indicator

/** BucketedKama 单测：桶内取最后值、跨桶推进一步、预热不足 None、与 KlineSeries+Kama 同源。 */
class BucketedKamaSpec extends munit.FunSuite:
  private val bucket = 1000L

  test("预热不足 (已结束桶 < erPeriod+1) -> None, 调用方须回退原始值"):
    val k = BucketedKama(bucket, erPeriod = 3)
    (0 until 3).foreach(i => k.update(i * bucket, i.toDouble)) // 只结束了 2 个桶
    assertEquals(k.value, None)
    assertEquals(k.efficiencyRatio, None)

  test("桶内只取最后一个样本 (中间抖动不进指标)"):
    val quiet = BucketedKama(bucket, erPeriod = 2)
    val noisy = BucketedKama(bucket, erPeriod = 2)
    // 两者每桶的**最后**样本相同, 但 noisy 桶内多喂了大幅抖动
    (0 to 5).foreach { i =>
      noisy.update(i * bucket, 1000.0) // 桶内噪声
      noisy.update(i * bucket + 500, i.toDouble)
      quiet.update(i * bucket, i.toDouble)
    }
    assertEquals(quiet.value, noisy.value)

  test("与 KlineSeries+Kama 同源 (同一序列, 同样的一步一值)"):
    val values = Seq(10.0, 11.0, 10.5, 12.0, 13.0, 12.5, 14.0, 15.0, 14.5, 16.0, 17.0, 16.5, 18.0, 19.0)
    val viaBars = new KlineSeries(bucket, 64) with Kama:
      override protected def kamaErPeriod: Int = 4
    val viaBucket = BucketedKama(bucket, erPeriod = 4)
    values.zipWithIndex.foreach { case (v, i) =>
      viaBars.update(i * bucket, v)
      viaBucket.update(i * bucket, v)
    }
    assertEquals(viaBucket.value, viaBars.kama)
    assertEquals(viaBucket.efficiencyRatio, viaBars.efficiencyRatio)

  test("趋势 ER→1 贴紧真值, 震荡 ER→0 几乎不动"):
    val trend = BucketedKama(bucket, erPeriod = 4, fast = 2, slow = 30)
    val chop = BucketedKama(bucket, erPeriod = 4, fast = 2, slow = 30)
    (0 to 20).foreach { i =>
      trend.update(i * bucket, i.toDouble)                          // 单边上行
      chop.update(i * bucket, if i % 2 == 0 then 0.0 else 1.0)      // 来回折返
    }
    val trendEr = trend.efficiencyRatio.getOrElse(fail("trend er"))
    val chopEr = chop.efficiencyRatio.getOrElse(fail("chop er"))
    assert(trendEr > 0.9, s"趋势 ER 应接近 1, 实为 $trendEr")
    assert(chopEr < 0.35, s"震荡 ER 应接近 0, 实为 $chopEr")
    // 趋势: KAMA 跟得紧 (与最新值差距小); 震荡: 停在中间, 不追最新值
    val trendLag = math.abs(trend.value.get - 20.0)
    assert(trendLag < 3.0, s"趋势下 KAMA 应贴紧真值, 滞后 $trendLag")

  test("稀疏样本不补空桶 (按有数据的桶推进, 与 KlineSeries 同口径)"):
    val k = BucketedKama(bucket, erPeriod = 2)
    Seq(0L, 5L, 100L, 1000L).foreach(b => k.update(b * bucket, b.toDouble)) // 桶号跳跃
    // 结束了 3 个桶 (值 0,5,100) -> erPeriod=2 已就绪。时间上跨了 1000 个桶宽, 但指标只推进了 3 步:
    // 空桶不补, 与 KlineSeries "按有成交的 bar 推进" 同口径。
    assert(k.value.nonEmpty, "3 个已结束桶 -> 应已就绪")
    val v = k.value.get
    assert(v > 0.0 && v < 100.0, s"KAMA 在基线与最新值之间, 实为 $v")

  test("桶宽非法 -> 抛错 (不静默按某个默认值跑)"):
    intercept[IllegalArgumentException](BucketedKama(0L))
