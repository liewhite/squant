package hft.exchange.bybit

import hft.domain.ExchangeError

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

  test("写单路径的错误归类: 限频 retCode -> RateLimited, 其余非 0 -> Rejected"):
    // Bybit 用 HTTP 200 + 顶层 retCode 表达业务结果, 必须在这一层分流 ——
    // 否则「保证金不足」(110007) 会被共享层判成"结果不确定"而杀掉整个进程。
    // 限频码依据 Bybit v5 错误码文档 (docs/v5/error)。
    def classify(retCode: Int) =
      BybitClient.classify(BybitClient.Fault(retCode, "x"), "下单", write = true)
    assert(classify(110007).isInstanceOf[ExchangeError.Rejected], classify(110007).toString)
    assert(classify(10006).isInstanceOf[ExchangeError.RateLimited], classify(10006).toString)
    assert(classify(10018).isInstanceOf[ExchangeError.RateLimited], classify(10018).toString)
    assert(classify(20003).isInstanceOf[ExchangeError.RateLimited], classify(20003).toString)
    // 读接口不做拒单/不确定的区分, 但限频仍要摘出来
    val read = BybitClient.classify(BybitClient.Fault(110007, "x"), "查挂单", write = false)
    assert(read.isInstanceOf[ExchangeError.Other], read.toString)
    val readLimited = BybitClient.classify(BybitClient.Fault(10006, "x"), "查挂单", write = false)
    assert(readLimited.isInstanceOf[ExchangeError.RateLimited], readLimited.toString)

  test("Binance 写单路径: 4xx -> Rejected, 5xx/网络 -> 原样 (结果不确定)"):
    import hft.exchange.binance.BinanceClient
    assert(BinanceClient.classifyWriteError(ExchangeError.Http(400, "-2019")).isInstanceOf[ExchangeError.Rejected])
    assert(BinanceClient.classifyWriteError(ExchangeError.Http(500, "oops")).isInstanceOf[ExchangeError.Http])
    assert(BinanceClient.classifyWriteError(ExchangeError.Network("timeout")).isInstanceOf[ExchangeError.Network])

  test("Binance 408/-1007: 执行状态未知, 绝不能判成拒单"):
    import hft.exchange.binance.BinanceClient
    assert(BinanceClient.classifyWriteError(ExchangeError.Http(408, "timeout")).isInstanceOf[ExchangeError.Http])
    val byCode = BinanceClient.classifyWriteError(ExchangeError.Http(400, """{"code":-1007,"msg":"Timeout"}"""))
    assert(byCode.isInstanceOf[ExchangeError.Http], byCode.toString)
