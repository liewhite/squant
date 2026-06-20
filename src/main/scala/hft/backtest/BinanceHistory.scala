package hft.backtest

import hft.domain.Symbol
import sttp.client4.SyncBackend

import java.nio.file.Path
import java.time.LocalDate

/** Binance 历史行情数据组装入口 —— 回测层统一负责**下载/缓存/组装**。
  *
  * 默认产出真实 trades 流 (trade-native，撮合走 [[hft.sim.SimState.matchTrade]])。
  * `synthesizeBbo=true` 时把 trade 替换为零价差 BBO ([[TradePrintBboSource]])，仅供写死依赖 BBO
  * 的策略且行情无 L1 时使用 (合成近似，会高估 maker 成交，默认不启用)。
  */
object BinanceHistory:
  def source(
      backend: SyncBackend,
      symbols: Seq[Symbol],
      start: LocalDate,
      end: LocalDate,
      synthesizeBbo: Boolean = false,
      cacheDir: String = "data-cache",
      kinds: Seq[BinanceDataKind] = BinanceDataKind.values.toIndexedSeq,
  ): MarketDataSource =
    val cache = LocalFsDataCache(Path.of(cacheDir))
    val downloader = BinanceHistoryDownloader(backend, cache)
    val base = BinanceHistorySource(downloader, symbols, start, end, kinds)
    if synthesizeBbo then TradePrintBboSource(base) else base
