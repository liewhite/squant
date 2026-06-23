package strategy.volsell.live

import java.nio.file.Files

/** 卖方配置 JSON 加载单测: 全字段解析、maxQty 缺省回落 baseQty×3、坏文件 Left、example 模板与模型一致。 */
class SellConfigSpec extends munit.FunSuite:

  private def withTemp(content: String)(f: String => Unit): Unit =
    val p = Files.createTempFile("sell-conf", ".json")
    try { Files.writeString(p, content); f(p.toString) }
    finally Files.deleteIfExists(p)

  test("Bybit: 全字段解析 + toVolSellConfig 映射"):
    val json =
      """{"apiKey":"k","apiSecret":"s","testnet":true,
        |"tuning":{"symbol":"ETHUSDT","baseCoin":"ETH","targetDays":14,"gridHigh":3.0,"gridLow":1.0,
        |"baseQty":2.0,"maxQty":9.0,"bars2w":1000,"runNow":true}}""".stripMargin
    withTemp(json) { path =>
      val c = SellConfig.loadBybit(path).fold(e => fail(e), identity)
      assertEquals(c.testnet, true)
      assertEquals(c.tuning.runNow, true)
      val v = c.tuning.toVolSellConfig
      assertEquals((v.symbol, v.baseCoin, v.targetDays, v.gridHigh, v.baseQty, v.bars2w, v.maxQty),
        ("ETHUSDT", "ETH", 14, 3.0, 2.0, 1000, 9.0))
    }

  test("maxQty 缺省 -> baseQty×3; 其余可选字段用默认"):
    val json = """{"apiKey":"k","apiSecret":"s","tuning":{"symbol":"ETHUSDT","baseCoin":"ETH","baseQty":2.0}}"""
    withTemp(json) { path =>
      val c = SellConfig.loadBybit(path).fold(e => fail(e), identity)
      assertEquals(c.testnet, false)               // 默认
      assertEquals(c.tuning.targetDays, 21)        // 默认
      assertEquals(c.tuning.runNow, false)         // 默认
      assertEquals(c.tuning.toVolSellConfig.maxQty, 6.0) // baseQty(2)×3
    }

  test("OKX: passphrase/quote/simulated"):
    val json = """{"apiKey":"k","apiSecret":"s","passphrase":"p","simulated":true,
                 |"tuning":{"symbol":"ETH","baseCoin":"ETH"}}""".stripMargin
    withTemp(json) { path =>
      val c = SellConfig.loadOkx(path).fold(e => fail(e), identity)
      assertEquals((c.passphrase, c.quote, c.simulated), ("p", "USDT", true)) // quote 默认 USDT
    }

  test("文件缺失 / 坏 JSON -> Left"):
    assert(SellConfig.loadBybit("/no/such.json").isLeft)
    withTemp("{ bad ]") { p => assert(SellConfig.loadOkx(p).isLeft) }

  test("example 模板与模型一致 (字段不漂移)"):
    assert(SellConfig.loadBybit("conf/vol-sell-bybit.example.json").isRight)
    assert(SellConfig.loadOkx("conf/vol-sell-okx.example.json").isRight)
