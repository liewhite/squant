package strategy.utils.hedge

import hft.TestUnits.given
import hft.domain.*

/** QuoteStyle 单测：被动挂在盘口外、跨价穿过盘口、TIF、紧迫度序、参数校验。 */
class QuoteStyleSpec extends munit.FunSuite:
  private val bbo = BBO(Exchange.Okx, "ETH", 2999.0, 1.0, 3001.0, 1.0, 0)

  test("被动: 卖挂 ask 之上、买挂 bid 之下 (PostOnly 不会被拒)"):
    val s = QuoteStyle.passive(0.01, 60_000)
    assertEquals(s.tif, TimeInForce.PostOnly)
    assertEquals(s.limitPrice(Side.Short, bbo), Price(3001.0 * 1.01))
    assertEquals(s.limitPrice(Side.Long, bbo), Price(2999.0 * 0.99))

  test("跨价: 卖挂 bid 之下、买挂 ask 之上 (到达即成交)"):
    val s = QuoteStyle.crossing(0.001, 1000)
    assertEquals(s.tif, TimeInForce.GTC, "PostOnly 会被拒 —— 跨价的用意就是要吃单")
    assertEquals(s.limitPrice(Side.Short, bbo), Price(2999.0 * 0.999))
    assertEquals(s.limitPrice(Side.Long, bbo), Price(3001.0 * 1.001))

  test("两种方式的卖价方向相反 —— 这正是它们的区别所在"):
    val sell = Side.Short
    val passive = QuoteStyle.passive(0.01, 60_000).limitPrice(sell, bbo)
    val cross = QuoteStyle.crossing(0.001, 1000).limitPrice(sell, bbo)
    assert(passive > Price(3001.0), s"被动卖价应在 ask 之上, 实为 $passive")
    assert(cross < Price(2999.0), s"跨价卖价应在 bid 之下, 实为 $cross")

  test("跨价比被动紧迫 (只允许由缓到急抢占)"):
    assert(QuoteStyle.crossing(0.001, 1000).urgency > QuoteStyle.passive(0.01, 60_000).urgency)


  test("零偏移合法 (挂在盘口上), 负偏移与非正 TTL 抛错"):
    assertEquals(QuoteStyle.passive(0.0, 1000).limitPrice(Side.Short, bbo), Price(3001.0))
    intercept[IllegalArgumentException](QuoteStyle.passive(-0.01, 1000))
    intercept[IllegalArgumentException](QuoteStyle.passive(0.01, 0))
    intercept[IllegalArgumentException](QuoteStyle.crossing(-0.01, 1000))
    intercept[IllegalArgumentException](QuoteStyle.crossing(0.01, -1))
