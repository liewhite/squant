package hft.exchange.okx

import hft.exchange.AccountReport
import OkxCodec.{AccountData, AccountDetail}

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
