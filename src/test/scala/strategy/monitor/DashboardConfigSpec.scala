package strategy.monitor

import hft.domain.Exchange
import hft.exchange.AccountMonitor

/** 配置契约: 按交易所分 case (填错字段是解析失败, 不是静默忽略)、逐家校验在任何连接之前跑完。 */
class DashboardConfigSpec extends munit.FunSuite:
  private def binance(k: String = "k", s: String = "s", poll: Long = 3000) =
    VenueConfig.Binance(k, s, Set("BTCUSDT"), mkt = true, poll)
  private def okx(passphrase: String = "p") =
    VenueConfig.Okx("k", "s", passphrase, Set("BTC-USDT-SWAP"), mkt = true, 3000)
  private def bybit() = VenueConfig.Bybit("k", "s", Set("BTCUSDT"), mkt = false, 5000)

  private def conf(vs: VenueConfig*) = DashboardConfig(vs.toVector, port = 8123)

  test("缺密钥即抛, 且指名哪一家"):
    assert(intercept[IllegalArgumentException](conf(binance(k = "")).validated).getMessage.contains("Binance"))
    intercept[IllegalArgumentException](conf(binance(s = "")).validated)

  test("OKX 的 passphrase 不能为空"):
    conf(okx()).validated // 不抛
    intercept[IllegalArgumentException](conf(okx(passphrase = "")).validated)

  test("轮询太快即抛 —— 账户接口有权重限制"):
    val e = intercept[IllegalArgumentException](conf(binance(poll = 10)).validated)
    assert(e.getMessage.contains("太快"), e.getMessage)
    conf(binance(poll = AccountMonitor.MinPollMs)).validated // 下限本身合法

  test("同一家配两次即抛"):
    val e = intercept[IllegalArgumentException](conf(binance(), binance()).validated)
    assert(e.getMessage.contains("两次"), e.getMessage)

  test("一家都没配即抛 —— 那个看板没有任何账户可看"):
    assert(intercept[IllegalArgumentException](conf().validated).getMessage.contains("至少要配一家"))

  test("顺序确定 —— 启动日志才可对比"):
    assertEquals(conf(okx(), bybit(), binance()).validated.map(_.exchange),
      Vector(Exchange.Binance, Exchange.Bybit, Exchange.Okx))

  test("Hyperliquid 压根没有那个 case —— 不是运行期再拒绝"):
    // 框架只有它的公共行情客户端, 给不出账户读数。做成"配置里表达不出来", 比配上之后
    // 在装配期 sys.exit 诚实: 后者要等到两家 client 都建好了才报。
    val hl = """{"port":8123,"exchanges":[{"venue":"Hyperliquid","k":"k","s":"s","syms":["ETH"],"mkt":true,"poll":3000}]}"""
    assert(DashboardConfig.load(writeTemp(hl)).isLeft, "Hyperliquid 不是可选的 venue, 应解析失败")

  test("填错字段是解析失败, 不是静默忽略"):
    // 并集形态 (passphrase/quote/accountType 都是 Option 再按交易所 match) 的问题在于
    // quote 填在 Binance 上会被静默忽略, 而配置者会一直以为它生效了。
    val onBinance = """{"port":8123,"exchanges":[{"venue":"Binance","k":"k","s":"s","syms":["BTCUSDT"],"mkt":true,"poll":3000,"quote":"USDT"}]}"""
    assert(DashboardConfig.load(writeTemp(onBinance)).isLeft, "Binance 没有 quote 字段, 填了应解析失败")

  test("模板配置能解析 (模板不该是跑不起来的配置)"):
    val c = DashboardConfig.load("conf/dashboard.example.json").fold(e => fail(e), identity)
    assertEquals(c.port, 8123)
    assertEquals(c.validated.map(_.exchange), Vector(Exchange.Binance, Exchange.Bybit, Exchange.Okx))
    assertEquals(c.validated.find(_.exchange == Exchange.Bybit).map(_.watchMarket), Some(false), "watchMarket 逐家可配")

  test("rootCauseChain: 把 ox 挂在 suppressed 上的真正原因挖出来"):
    // 装配失败最常见的原因是密钥填错, 而 ox 把它挂在 suppressed 上, 打印时还会变成
    // CIRCULAR REFERENCE —— 操作者要读四十行栈才知道是密钥的事。
    val outer = RuntimeException("scope failed")
    outer.addSuppressed(IllegalStateException("HTTP 401: API-key format invalid"))
    assert(rootCauseChain(outer).contains("HTTP 401: API-key format invalid"))

  test("rootCauseChain: 自引用不死循环, 同一条消息只出现一次"):
    val selfRef = new RuntimeException("boom") { override def getCause: Throwable = this }
    assertEquals(rootCauseChain(selfRef), Vector("boom"))
    assertEquals(rootCauseChain(RuntimeException("same", RuntimeException("same"))), Vector("same"))

  private def writeTemp(json: String): String =
    val f = java.nio.file.Files.createTempFile("dashboard-conf", ".json")
    java.nio.file.Files.writeString(f, json)
    f.toString

  test("rootCauseChain: 只是转述内层的外层消息被丢掉, 不把同一句说两遍"):
    // 包装异常的 getMessage 常常就是 "java.lang.IllegalStateException: <内层原话>"。
    val inner = IllegalStateException("HTTP 401: API-key format invalid")
    val outer = RuntimeException(inner.toString, inner)
    assertEquals(rootCauseChain(outer), Vector("HTTP 401: API-key format invalid"))
