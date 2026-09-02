package hft.exchange.bybit

import hft.exchange.AccountReport
import BybitCodec.{WalletCoin, WalletData}

/** Bybit `wallet` 推送 -> [[AccountReport]] 的映射契约。 */
class BybitAccountFeedSpec extends munit.FunSuite:
  private val ts = 1_700_000_000_000L

  test("wallet 推送逐币种报当前余额 —— 不声称这是整份钱包"):
    // Bybit 文档只写了"订阅成功时不给 snapshot", 从未声明 coin[] 覆盖全部币种;
    // 官方示例里 totalWalletBalance 远大于唯一列出的那条的 usdValue, 自己就反证了。
    // 且 Bybit **没有**周期性全量推送 —— 当成全量抹掉的 ETH 现货会一直错到它自己再变一次。
    val push = WalletData(accountType = "UNIFIED", totalEquity = "9684.46", coin = List(WalletCoin("USDT", "500.5")))
    assertEquals(
      BybitAccountFeed.walletReports(push, ts),
      Vector(AccountReport.EquityChanged(9684.46, ts), AccountReport.BalanceChanged("USDT", 500.5, ts)),
    )

  test("多币种推送: 每个币种各一条 BalanceChanged"):
    val push = WalletData(totalEquity = "1.0", coin = List(WalletCoin("USDT", "500.0"), WalletCoin("ETH", "2.5")))
    assertEquals(
      BybitAccountFeed.walletReports(push, ts).collect { case b: AccountReport.BalanceChanged => (b.currency, b.amount) },
      Vector(("USDT", 500.0), ("ETH", 2.5)),
    )
