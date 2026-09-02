package hft.exchange

import hft.domain.*
import hft.event.Commands.{AccountSyncRequest, AccountSynced}
import hft.event.Topics
import hft.TestUnits.given

/** 启动对齐事件序列的**顺序契约**: 持仓 -> 净值 -> 钱包 -> 既有挂单 -> 完成应答。
  *
  * 应答放行行情，所以它必须排在最后；排错了就等于"对齐没做完却已经开始交易"。 */
class SyncEventsSpec extends munit.FunSuite:
  private val request = AccountSyncRequest(AccountId.Live, Exchange.Okx, requestId = 7L, symbols = Set("ETH-USDT-SWAP"))
  private val info = AccountInfo(AccountId.Live, Exchange.Okx, equity = 10_000.0)

  private def events(wallet: Map[String, Double]) =
    TradingGateway.syncEvents(
      request,
      Exchange.Okx,
      positions = Vector.empty,
      accountInfo = info,
      wallet = wallet,
      pendingOrders = Vector.empty,
    )

  test("钱包快照在对齐序列里, 且排在完成应答之前"):
    // 钱包是全量事实的唯一来源 (三家 WS 都只覆盖变动币种, 见 hft.domain.Wallet)。
    // 排在应答之后就等于"行情已放行、策略却还不知道现货有多少" —— delta 对冲会按缺现货的敞口下单。
    val evs = events(Map("USDT" -> 500.0, "ETH" -> 2.0))
    val walletIdx = evs.indexWhere(_.as(Topics.Wallet).isDefined)
    val syncedIdx = evs.indexWhere(_.as(AccountSynced).isDefined)
    assert(walletIdx >= 0, "对齐序列必须包含钱包快照")
    assert(walletIdx < syncedIdx, s"钱包 (@$walletIdx) 必须排在完成应答 (@$syncedIdx) 之前")
    assertEquals(syncedIdx, evs.size - 1, "完成应答必须是最后一条")

  test("钱包快照如实带上全部币种; 空钱包也发 —— 那正是'全部余额都是 0'这个事实"):
    val wallet = events(Map("USDT" -> 500.0, "ETH" -> 2.0)).flatMap(_.as(Topics.Wallet)).head
    assertEquals(wallet.balances, Map("USDT" -> 500.0, "ETH" -> 2.0))
    assertEquals(wallet.exchange, Exchange.Okx)
    assertEquals(wallet.account, AccountId.Live)
    // 一个只卖期权、不持现货的账户拿到的就是空表。不发的话下游永远分不清它与"还没对齐"。
    assertEquals(events(Map.empty).flatMap(_.as(Topics.Wallet)).head.balances, Map.empty[String, Double])

  test("顺序: 净值先于钱包 (两者都是账户级读数, 但风控读净值)"):
    val evs = events(Map("USDT" -> 1.0))
    assert(evs.indexWhere(_.as(Topics.AccountInfo).isDefined) < evs.indexWhere(_.as(Topics.Wallet).isDefined))
