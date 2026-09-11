package hft.exchange.okx

import hft.exchange.AccountReport
import OkxCodec.{AccountData, AccountDetail}
import hft.TestUnits.given

/** OKX `account` 推送 -> [[AccountReport]] 的映射契约。
  *
  * 这条映射决定 delta 对冲把多少现货算进敞口，是本连接器唯一的业务判断。 */
class OkxAccountFeedSpec extends munit.FunSuite:

  test("account 推送逐币种报当前余额 —— 不声称这是整份钱包"):
    // OKX 文档: 只有 initial/regular snapshot 是全量, 变动触发的 event_update 只带那一个币种
    // ("only the incremental data of that currency will be pushed"), 且快照可能按 curPage/lastPage 分页。
    // 报文里没有 eventType/分页字段可读, 所以这条通道给不出"全量"这个事实。
    val push = AccountData(uTime = "1700000000000", totalEq = "12345.6", details = List(AccountDetail("USDT", "500.5")))
    val reports = OkxAccountFeed.accountReports(push)
    assertEquals(
      reports,
      Vector(
        AccountReport.EquityChanged(12345.6, 1_700_000_000_000L),
        AccountReport.BalanceChanged("USDT", 500.5, 1_700_000_000_000L),
      ),
      "只有 USDT 变动的一条 event_update 不能被报成'整份钱包只有 USDT' —— " +
        "那会让 ETH 现货被抹成 0, 对冲随即按少算整份现货的 delta 下单",
    )

  test("多币种推送: 每个币种各一条 BalanceChanged"):
    val push = AccountData(
      uTime = "1700000000000",
      totalEq = "1.0",
      details = List(AccountDetail("USDT", "500.0"), AccountDetail("ETH", "2.5")),
    )
    assertEquals(
      OkxAccountFeed.accountReports(push).collect { case b: AccountReport.BalanceChanged => (b.currency, b.amount) },
      Vector(("USDT", 500.0), ("ETH", 2.5)),
    )

  test("缺 uTime 即抛 —— 时间戳是陈旧判定的依据, 填默认值等于宣称'刚刚收到'"):
    val push = AccountData(uTime = "", totalEq = "1.0")
    assert(intercept[IllegalStateException](OkxAccountFeed.accountReports(push)).getMessage.contains("uTime"))

  // ==================== orders 频道: 哪些回报归本柜台管 ====================

  /** 私有 orders 频道按 `instType: SWAP` **全量**订阅, 所以这一层要回答"这条回报是不是
    * 我的"。它决定引擎活不活: 从前认不出的 instId 直接抛, 而账户上一张别人的单就能触发。 */
  private val instrumentsJson =
    """{"code":"0","msg":"","data":[
      |{"instId":"ETH-USDT-SWAP","tickSz":"0.01","lotSz":"0.1","minSz":"0.1","ctVal":"0.1","state":"live"}
      |]}""".stripMargin

  private def ordersPush(instId: String, clOrdId: String): String =
    s"""{"arg":{"channel":"orders","instType":"SWAP"},"data":[{
       |"instId":"$instId","ordId":"o1","clOrdId":"$clOrdId","side":"buy","state":"live",
       |"reduceOnly":"false","px":"2000","sz":"1","fillSz":"0","fillPx":"0","accFillSz":"0","avgPx":""
       |}]}""".stripMargin

  /** 接好 sink 的私有流。`spawn` 吞掉 —— WsLoop 与 greeks 轮询因此都不真跑。 */
  private def feedWithSink(): (OkxAccountFeed, scala.collection.mutable.ArrayBuffer[AccountReport]) =
    val backend = sttp.client4.testing.BackendStub.synchronous.whenAnyRequest
      .thenRespondF(_ => sttp.client4.testing.ResponseStub.adjust(instrumentsJson))
    val ws = sttp.client4.DefaultSyncBackend()
    val feed = OkxAccountFeed(OkxClient.trading(backend, OkxCredentials("k", "s", "p")), ws)
    val reports = scala.collection.mutable.ArrayBuffer.empty[AccountReport]
    feed.connect(reports += _, _ => (), _ => true)
    (feed, reports)

  test("本 quote 的永续: 照常报出来"):
    val (feed, reports) = feedWithSink()
    feed.onPrivateText(ordersPush("ETH-USDT-SWAP", "myorder1"))
    assertEquals(reports.size, 1, reports.toString)
    assertEquals(
      reports.head.asInstanceOf[AccountReport.OrderStatusChanged].instrument,
      hft.domain.Instrument.perp(hft.domain.Exchange.Okx, "ETH"),
    )

  test("币本位的单: 跳过, 不终止引擎 —— 哪怕它带着 clOrdId"):
    // 同账户上别的 API 客户 (另一个机器人、网格工具) 下的单也带**它自己的** clOrdId,
    // 而我们的 OKX clOrdId 是无前缀的 32 位 hex, 报文里没有任何东西能证明它是我们的。
    // 所以"带 clOrdId 即我们的单"不成立, 拿它当归属判据只是把"别人的单杀掉引擎"换个范围。
    val (feed, reports) = feedWithSink()
    feed.onPrivateText(ordersPush("ETH-USD-SWAP", ""))          // 手工单, 无 clOrdId
    feed.onPrivateText(ordersPush("ETH-USD-SWAP", "someoneelse")) // 别的客户端下的单
    feed.onPrivateText(ordersPush("ETH-USDC-SWAP", "other"))     // 别的计价币
    assertEquals(reports.size, 0, "都不归本柜台管, 一条都不该报")
