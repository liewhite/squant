package hft.backtest.binance

import hft.backtest.{LocalFsDataCache, MarketDataKind, MarketDataProvider, MarketDataSource}
import hft.domain.{Exchange, Symbol}
import sttp.client4.SyncBackend

import java.nio.file.Path
import java.time.LocalDate

/** 币安 U 本位合约的历史行情供应商 —— data.binance.vision 每日 zip，带本地缓存。
  *
  * 数据可得性 (实测 2026-08)：`trades` 持续更新；`bookTicker` 每日文件**止于 2024-03-30**，
  * 此后无 L1 历史。故该日期之后的回测若需 BBO，要么换一个 [[MarketDataProvider]] 实现
  * (接别的数据商)，要么显式套 [[hft.backtest.SyntheticBboSource]] 用逐笔成交合成盘口。
  * 这里**不做静默兜底** —— 缺数据就是缺数据，由装配方明示如何补，回测结果才可信。
  */
final class BinanceMarketDataProvider(downloader: BinanceHistoryDownloader) extends MarketDataProvider:
  override def exchange: Exchange = Exchange.Binance

  override def source(
      symbols: Seq[Symbol],
      start: LocalDate,
      end: LocalDate,
      kinds: Set[MarketDataKind] = MarketDataKind.All,
  ): MarketDataSource =
    BinanceDailySource(downloader, symbols, start, end, kinds)

object BinanceMarketDataProvider:
  val DefaultCacheDir = "data-cache"

  /** 常用装配：HTTP 后端 + 本地文件系统缓存。 */
  def apply(backend: SyncBackend, cacheDir: String = DefaultCacheDir): BinanceMarketDataProvider =
    new BinanceMarketDataProvider(BinanceHistoryDownloader(backend, LocalFsDataCache(Path.of(cacheDir))))
