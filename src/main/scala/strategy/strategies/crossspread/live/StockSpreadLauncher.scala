package strategy.strategies.crossspread.live

import hft.dashboard.DashboardActor
import hft.domain.{Exchange, ExchangeError, Instrument, Symbol}
import hft.engine.Engine
import hft.event.{Interest, Topics}
import hft.exchange.binance.{BinanceClient, BinanceMarketFeed}
import hft.exchange.hyperliquid.{HyperliquidClient, HyperliquidMarketFeed}
import hft.exchange.okx.{OkxClient, OkxMarketFeed}
import org.slf4j.LoggerFactory
import ox.{fork, supervised}
import sttp.client4.DefaultSyncBackend
import strategy.strategies.crossspread.logic.*

/** 币安 / OKX / Hyperliquid 三所**股票永续价差监控**启动器 —— 只看不交易，无需 API key。
  *
  * 盯住三家所共同上市的传统资产永续 (股票、ETF、商品、盘前)，在**价差突然偏离它自己的
  * 中枢**时报一条日志。持续存在的价差不报：那是结构性的 (资金费率、参与者、上市时间差)，
  * 照着它开仓等来的不是回归而是持仓成本。
  *
  * 装配上没有柜台：**"能不能下单"因此是装配期的事实**，不是运行期的自觉 ——
  * 即使有人往里塞一个会发下单指令的组件，指令也没有处理者，装配当场失败。
  *
  * 运行：`sbt "runMain strategy.strategies.crossspread.live.StockSpreadLauncher [minZ] [minDevBps] [代码白名单] [看板端口]"`
  *   - `minZ`         偏离多少个标准差才报，默认 5
  *   - `minDevBps`    偏离至少多少 bp 才报，默认 15
  *   - `代码白名单`   逗号分隔 (如 `AAPL,NVDA,TSLA`)，缺省则监控全部共同上市的标的
  *   - `看板端口`     实时看板端口，默认 8123；给 `0` 表示不起看板
  *
  * 看板 (见 `docs/dashboard.md`) 显示的是**每个交易所每个标的的盘口与它的年龄** ——
  * 对这个监控进程来说, 最有用的恰恰是年龄那一列: 哪一家的行情停了、哪些标的根本没在报价,
  * 一眼看得出来, 而心跳日志只给得出汇总数。**它不显示价差与异动** ——
  * 价差对是本策略自己的领域概念, 走 `SpreadDislocations` topic 与上面的日志。
  *
  * 注意预热：默认 `sampleMs=1000 × minSamples=300` 即 **5 分钟**之后才会有第一条信号，
  * 之前所有价差对都在攒中枢 (宁可不报，也不拿几十个样本估出来的中枢去判"偏离")。
  */
/** 由各所清单反建 (交易所, Symbol) -> 标的代码。认不出的标的退回它自己的 symbol ——
  * 那只可能是看板收到了不在本次宇宙里的行情, 如实按 symbol 单独成行即可。 */
private def tickerOf(listings: Map[Exchange, Map[Ticker, Symbol]]): Instrument => Ticker =
  val index: Map[Instrument, Ticker] =
    listings.iterator.flatMap((exchange, bySymbol) =>
      bySymbol.iterator.map((ticker, symbol) => Instrument(exchange, symbol) -> ticker)
    ).toMap
  instrument => index.getOrElse(instrument, instrument.symbol)

@main def StockSpreadLauncher(args: String*): Unit =
  val logger = LoggerFactory.getLogger("StockSpreadLauncher")
  val minZ = args.lift(0).map(_.toDouble).getOrElse(5.0)
  val minDeviationBps = args.lift(1).map(_.toDouble).getOrElse(15.0)
  val whitelist: Set[Ticker] =
    args.lift(2).map(_.split(',').iterator.map(_.trim.toUpperCase).filter(_.nonEmpty).toSet).getOrElse(Set.empty)
  val dashboardPort = args.lift(3).map(_.toInt).getOrElse(8123)

  def orExit[A](what: String)(result: Either[ExchangeError, A]): A = result match
    case Right(value) => value
    case Left(e)      => logger.error(s"$what 失败: ${e.message}"); sys.exit(1)

  supervised:
    val backend = DefaultSyncBackend()
    // 三家都只用公开行情, 不需要凭证
    val binance = BinanceClient.public(backend)
    val okx = OkxClient.public(backend) // 计价币 USDT
    val hyperliquid = HyperliquidClient(backend, HyperliquidClient.StockDex)

    // ==================== 标的宇宙: 交易所自己才知道当前上市了什么, 不写死清单 ====================

    // 币安与 Hyperliquid 各自能回答"这是传统资产永续吗"; OKX 的清单里加密与股票混在一起,
    // 故它只按代码加入前两者给出的候选 (判据与最终防线见 CrossVenueUniverse / PairRejection)
    val binanceTradFi = orExit("取币安传统资产永续清单")(binance.fetchTradFiPerps())
    val hyperliquidStocks = orExit(s"取 Hyperliquid ${HyperliquidClient.StockDex} dex 清单")(hyperliquid.listedSymbols())
    val okxSwaps = okx.symbolMetas.keySet // 拉不到即抛, 装配期失败好过带着半个宇宙上线

    val candidates = (binanceTradFi.keySet ++ hyperliquidStocks).filter(t => whitelist.isEmpty || whitelist(t))
    val listings: Map[Exchange, Map[Ticker, Symbol]] = Map(
      Exchange.Binance -> binanceTradFi,
      // OKX 与 Hyperliquid 的框架 Symbol 就是基础资产代码本身 (见各自的 Codec)
      Exchange.Okx -> okxSwaps.iterator.map(s => s -> s).toMap,
      Exchange.Hyperliquid -> hyperliquidStocks.iterator.map(s => s -> s).toMap,
    )
    val pairs = CrossVenueUniverse.pairs(candidates, listings)
    if pairs.isEmpty then
      logger.error(s"没有任何标的同时在两家以上上市 (候选 ${candidates.size} 个), 无价差可算")
      sys.exit(1)

    val detector = CrossSpreadDetector(pairs, CrossSpreadConfig(), ZScoreRule(minZ, minDeviationBps))
    val monitor = CrossSpreadMonitor(detector)

    // 消费者先起、生产者后起：监控器要先挂在总线上, 否则最早那批盘口没人接
    Engine.run(plugins =
      Vector(
        BinanceMarketFeed(backend),
        OkxMarketFeed(okx, backend),
        HyperliquidMarketFeed(backend, HyperliquidClient.StockDex),
      )
    ) { engine =>
      engine.install(monitor)
      // 0 = 不起看板。端口被占时它会在装配期抛 (见 DashboardActor) —— 那通常说明
      // 上一个进程还活着, 而两个进程同时盯同一批标的只会让日志更难读。
      //
      // 把 listings 反过来喂给看板: 各所的 symbol 串本就不同 (AAPLUSDT / AAPL / AAPL),
      // 而"哪两个是同一个资产"的**权威答案就在这张表里** —— 它是从三家交易所的清单建出来的。
      // 让看板自己去猜 (去掉 USDT 后缀之类) 等于把这份知识抄第二遍, 而且是会猜错的那种抄法。
      if dashboardPort > 0 then engine.install(DashboardActor(dashboardPort, assetOf = tickerOf(listings)))
      // 不占标的、不做启动对齐: 这些标的谁都可以拿去交易, 监控器只是在看
      engine.watchMarket(detector.instruments, Set(Topics.Bbo))

      if dashboardPort > 0 then logger.warn(s"看板: http://127.0.0.1:$dashboardPort")
      val perVenue = detector.instruments.groupBy(_.exchange).map((e, is) => s"$e ${is.size}").toVector.sorted
      logger.warn(
        s"监控 ${pairs.size} 个价差对 / ${detector.instruments.size} 个标的 (${perVenue.mkString(", ")}) " +
          s"| 均线窗口 ${detector.config.sampleMs * detector.config.windowSamples / 1000}s " +
          s"| 预热 ${detector.config.warmupMs / 1000}s (完成前不报) " +
          s"| 阈值 z>=$minZ 且 偏离>=${minDeviationBps}bp"
      )

      // 旁路观察者：把异动打到控制台。策略消费同一条 topic 即可, 这里只做人看的输出。
      val mailbox = engine.subscribe(Set(Interest.All(SpreadDislocations)))
      fork {
        mailbox.events.foreach { ev =>
          ev.as(SpreadDislocations).foreach(d => logger.warn(SpreadDislocation.describe(d)))
        }
      }

      // 心跳：预热进度与累计报警数, 让"还没到点"与"接线错了"能区分开
      fork {
        while true do
          Thread.sleep(30_000)
          val s = monitor.stats
          logger.warn(
            s"[心跳] 盘口 ${s.quotesSeen} 条 | 价差对 ${s.pairsTracked} 个 | " +
              s"预热就绪 ${s.pairsReady}/${s.pairsTracked} | 剔除 ${s.pairsRejected} | 累计报警 ${s.alertsEmitted}"
          )
      }

      // 阻塞到停机: 中断信号或组件失败都会唤醒它, 停完全部组件后核心最后退出
      engine.awaitShutdown()
    }
