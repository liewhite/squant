package hft.exchange.okx

import sttp.client4.testing.SyncBackendStub

/** OKX 签名单测：HMAC-SHA256 + Base64 编码、WS 登录签名串拼装。
  * (签名是实盘鉴权的核心且为纯函数，用已知 HMAC 向量独立验证，非自证。)
  *
  * 另含合约规格解析的一条回归 —— 见"未上市合约"那个用例。
  */
class OkxClientSpec extends munit.FunSuite:

  // 经典 HMAC-SHA256 测试向量:
  //   HMAC-SHA256(key="key", "The quick brown fox jumps over the lazy dog")
  //   = f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8 (hex)
  //   base64(那 32 字节) = 97yD9DBThCSxMpjmqm+xQ+9NWaFJRhdZl0edvC0aPNg=
  test("hmacSha256Base64 = base64(HMAC-SHA256(secret, data)) (已知向量)"):
    val got = OkxClient.hmacSha256Base64("key", "The quick brown fox jumps over the lazy dog")
    assertEquals(got, "97yD9DBThCSxMpjmqm+xQ+9NWaFJRhdZl0edvC0aPNg=")
    // 32 字节 -> 44 字符 base64 (含 '=' 补位)
    assertEquals(got.length, 44)

  test("WS 登录签名 = base64(HMAC(secret, ts + \"GET/users/self/verify\"))"):
    val creds = OkxCredentials(apiKey = "k", secret = "topsecret", passphrase = "p")
    val ts = "1700000000"
    assertEquals(creds.signWsLogin(ts), OkxClient.hmacSha256Base64("topsecret", s"${ts}GET/users/self/verify"))

  test("签名确定性: 相同输入相同输出, 不同密钥不同输出"):
    assertEquals(OkxClient.hmacSha256Base64("s", "msg"), OkxClient.hmacSha256Base64("s", "msg"))
    assertNotEquals(OkxClient.hmacSha256Base64("s1", "msg"), OkxClient.hmacSha256Base64("s2", "msg"))

  // ==================== 合约规格 ====================

  /** 真实响应节选 (2026-09)：一个已上市合约 + 一个预告上市 (`preopen`) 的合约。
    *
    * 后者的规格字段全是**空字符串** —— 不是 "0"。它曾让每一个 OKX 客户端在启动拉规格时
    * 抛 `Invalid number from OKX API: ''` 而崩溃：交易所预告一个与本进程毫不相干的新合约，
    * 就能让进程起不来。
    */
  private val instrumentsBody =
    """{"code":"0","msg":"","data":[
      {"instId":"BTC-USDT-SWAP","state":"live","tickSz":"0.1","lotSz":"0.01","minSz":"0.01","ctVal":"0.01"},
      {"instId":"JP225-USDT-SWAP","state":"preopen","tickSz":"","lotSz":"","minSz":"","ctVal":"","instCategory":""}
    ]}"""

  test("只取已上市合约: 预告上市的合约规格字段为空串, 读它就是崩溃"):
    val backend = SyncBackendStub.whenAnyRequest.thenRespondAdjust(instrumentsBody)
    val metas = OkxClient.public(backend).fetchAllSymbolMetas()
    assertEquals(metas.map(_.map(_.symbol)), Right(Vector("BTC")))
