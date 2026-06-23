package strategy.makerhedge.live

import java.nio.file.Files

/** 对冲配置 JSON 加载单测: 全字段解析、可选字段缺省回填、文件缺失/坏 JSON 显式 Left。 */
class HedgeConfigSpec extends munit.FunSuite:

  private def withTemp(content: String)(f: String => Unit): Unit =
    val p = Files.createTempFile("hedge-conf", ".json")
    try
      Files.writeString(p, content)
      f(p.toString)
    finally Files.deleteIfExists(p)

  test("Bybit: 全字段解析"):
    val json =
      """{"apiKey":"k","apiSecret":"s","testnet":true,
        |"tuning":{"symbol":"ETHUSDT","ccy":"ETH","klineBar":"60","greeksPollMs":2000,
        |"offset":0.0003,"requoteMs":4000,"tightAtr":1.5,"looseAtr":2.5,"maxHedgeQty":7.0}}""".stripMargin
    withTemp(json) { path =>
      val c = HedgeConfig.loadBybit(path).fold(e => fail(e), identity)
      assertEquals(c.apiKey, "k")
      assertEquals(c.testnet, true)
      assertEquals(c.tuning, HedgeTuning("ETHUSDT", "ETH", "60", 2000, 0.0003, 4000, 1.5, 2.5, 7.0))
    }

  test("Bybit: 可选字段缺省回填 (testnet + tuning 调参用默认)"):
    val json = """{"apiKey":"k","apiSecret":"s","tuning":{"symbol":"ETHUSDT","ccy":"ETH","klineBar":"60"}}"""
    withTemp(json) { path =>
      val c = HedgeConfig.loadBybit(path).fold(e => fail(e), identity)
      assertEquals(c.testnet, false)             // 默认
      assertEquals(c.tuning.greeksPollMs, 3000L) // 默认
      assertEquals(c.tuning.maxHedgeQty, 5.0)    // 默认
      assertEquals(c.tuning.tightAtr, 1.0)       // 默认
    }

  test("OKX: passphrase/quote/simulated 解析与缺省"):
    val full = """{"apiKey":"k","apiSecret":"s","passphrase":"p","quote":"USDC","simulated":true,
                 |"tuning":{"symbol":"ETH","ccy":"ETH","klineBar":"1H"}}""".stripMargin
    withTemp(full) { path =>
      val c = HedgeConfig.loadOkx(path).fold(e => fail(e), identity)
      assertEquals((c.passphrase, c.quote, c.simulated), ("p", "USDC", true))
    }
    val minimal = """{"apiKey":"k","apiSecret":"s","passphrase":"p","tuning":{"symbol":"ETH","ccy":"ETH","klineBar":"1H"}}"""
    withTemp(minimal) { path =>
      val c = HedgeConfig.loadOkx(path).fold(e => fail(e), identity)
      assertEquals((c.quote, c.simulated), ("USDT", false)) // 默认
    }

  test("文件缺失 / 坏 JSON -> Left (不抛, 不静默)"):
    assert(HedgeConfig.loadBybit("/no/such/conf.json").isLeft)
    withTemp("{ not json ]") { path => assert(HedgeConfig.loadOkx(path).isLeft) }

  test("example 模板与代码模型一致 (字段不漂移)"):
    assert(HedgeConfig.loadBybit("conf/perp-hedge-bybit.example.json").isRight)
    assert(HedgeConfig.loadOkx("conf/perp-hedge-okx.example.json").isRight)
