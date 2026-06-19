package hft.exchange.bybit

/** Bybit 签名单测：HMAC-SHA256 + 十六进制编码、WS auth 签名串拼装。
  * (签名是实盘鉴权的核心且为纯函数，用已知 HMAC 向量独立验证，非自证。)
  */
class BybitClientSpec extends munit.FunSuite:

  // 经典 HMAC-SHA256 测试向量:
  //   HMAC-SHA256(key="key", "The quick brown fox jumps over the lazy dog")
  //   = f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8 (hex)
  test("hmacSha256Hex = 小写 hex(HMAC-SHA256(secret, data)) (已知向量)"):
    val got = BybitClient.hmacSha256Hex("key", "The quick brown fox jumps over the lazy dog")
    assertEquals(got, "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8")
    // 32 字节 -> 64 字符 hex
    assertEquals(got.length, 64)

  test("WS auth 签名 = hex(HMAC(secret, \"GET/realtime\" + expires))"):
    val creds = BybitCredentials(apiKey = "k", apiSecret = "topsecret")
    val expires = 1700000000000L
    assertEquals(creds.signWsAuth(expires), BybitClient.hmacSha256Hex("topsecret", s"GET/realtime$expires"))

  test("签名确定性: 相同输入相同输出, 不同密钥不同输出"):
    assertEquals(BybitClient.hmacSha256Hex("s", "msg"), BybitClient.hmacSha256Hex("s", "msg"))
    assertNotEquals(BybitClient.hmacSha256Hex("s1", "msg"), BybitClient.hmacSha256Hex("s2", "msg"))
