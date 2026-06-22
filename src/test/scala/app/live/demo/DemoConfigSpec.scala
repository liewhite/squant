package app.live.demo

import java.nio.file.Files

/** demo 可选配置: 文件缺失 -> 空 (公共行情可跑)、存在则解析、hasCreds 判定、example 模板一致。 */
class DemoConfigSpec extends munit.FunSuite:

  test("文件不存在 -> 空配置 (无密钥, 非 live)"):
    val c = DemoConfig.loadOrEmpty("/no/such/demo.json")
    assertEquals(c, DemoConfig("", "", false))
    assert(!c.hasCreds)

  test("文件存在 -> 解析; hasCreds 仅在 key+secret 都非空时为真"):
    val p = Files.createTempFile("demo-conf", ".json")
    try
      Files.writeString(p, """{"apiKey":"k","apiSecret":"s","live":true}""")
      val c = DemoConfig.loadOrEmpty(p.toString)
      assertEquals((c.apiKey, c.live, c.hasCreds), ("k", true, true))
    finally Files.deleteIfExists(p)
    assert(!DemoConfig("k", "", false).hasCreds) // 缺 secret

  test("example 模板可解析"):
    assert(util.Try(DemoConfig.loadOrEmpty("conf/demo.example.json")).isSuccess)
