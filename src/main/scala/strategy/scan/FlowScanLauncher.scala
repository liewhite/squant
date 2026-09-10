package strategy.scan

import hft.domain.InstrumentKind
import hft.domain.{Exchange, Instrument}
import hft.engine.Engine
import hft.event.{Interest, Topics}
import hft.exchange.binance.{BinanceClient, BinanceMarketFeed}
import org.slf4j.LoggerFactory
import ox.{fork, supervised}
import sttp.client4.DefaultSyncBackend

/** 全市场 taker 流向扫描器启动器 —— 只看不交易，无需 API key。
  *
  * 盯住币安**全部** USDT 永续（撰写时 527 个）的逐笔成交，报出独立于大盘的单向爆量。
  * 不下单、不需要凭证：走的是公开行情流，`Engine` 在没有 accountStream 时退化为纯研究模式。
  *
  * 装配上的两处要点，都不是随手写的：
  *   - 用 [[Engine.watchMarket]] 而不是 `addStrategy`。后者会把这 500 多个标的全部独占登记，
  *     之后针对它们的交易策略一个都起不来 —— 而扫描器的目的正是给交易策略挑标的。
  *   - 扫描器是 [[hft.actor.Actor]] 不是 `Strategy`，以 `Interest.All(Topics.Trade)` 收全市场。
  *
  * 运行：`sbt "runMain strategy.scan.FlowScanLauncher [residualZ] [minNotional]"`
  *
  * 注意基线需要预热：默认 `bucketMs=1000 × baselineSamples=600` 即 **10 分钟**之后才会有第一条
  * 信号，之前所有标的的 z 分都是 None（宁可不报，也不拿几个样本估出来的尺度下结论）。
  */
@main def FlowScanLauncher(args: String*): Unit =
  val logger = LoggerFactory.getLogger("FlowScanLauncher")
  val residualZ = args.lift(0).map(_.toDouble).getOrElse(4.0)
  val minNotional = args.lift(1).map(_.toDouble).getOrElse(50_000.0)

  supervised:
    val backend = DefaultSyncBackend()
    val client = BinanceClient.public(backend) // 只读公开行情, 无需凭证
    // 只装行情插件, 不装柜台 —— 扫描器不交易, "能不能下单"因此是装配期的事实。
    Engine.run(plugins = Vector(BinanceMarketFeed(backend))) { engine =>

      // 全部 USDT 永续 —— 交易所自己才知道当前上市了哪些合约, 不写死清单。
      val instruments: Set[Instrument] = client.fetchMetas(InstrumentKind.LinearPerp) match
        case Right(metas) => metas.filter(_.symbol.endsWith("USDT")).map(_.instrument).toSet
        // 抛而不是 sys.exit: 后者直接杀 JVM, 会跳过 Engine.run 的有序停机
        case Left(e) => throw IllegalStateException(s"取合约列表失败: ${e.message}")

      // 观测参数（窗口/基线）与判定参数（阈值）分属两处 —— 换判定规则不必碰观测配置
      val config = FlowScanConfig()
      val rule = CrossSectionalMedianRule(residualZ = residualZ, minWindowNotional = minNotional)
      val scanner = FlowScanner(Exchange.Binance, config, rule)

      // 消费者先起、生产者后起：扫描器要先挂在总线上, 否则最早那批成交没人接
      engine.install(scanner)
      engine.watchMarket(instruments, Set(Topics.Trade))

      logger.warn(
        s"扫描 ${instruments.size} 个 USDT 永续 | 窗口 ${config.bucketMs * config.windowBuckets / 1000}s " +
          s"| 基线 ${config.bucketMs * config.baselineSamples / 1000}s (预热完成前不报) " +
          s"| 阈值 |z|>=$residualZ | 名义额下限 $minNotional"
      )

      // 旁路观察者：把异动打到控制台。策略消费同一条 topic 即可, 这里只做人看的输出。
      val mailbox = engine.subscribe(Set(Interest.All(FlowAnomalies)))
      fork {
        mailbox.events.foreach { ev =>
          ev.as(FlowAnomalies).foreach(a => logger.warn(FlowScanner.describe(a)))
        }
      }

      // 心跳：预热进度与累计报警数, 让"还没到点"与"接线错了"能区分开
      fork {
        while true do
          Thread.sleep(30_000)
          val s = scanner.stats
          logger.warn(
            s"[心跳] 成交 ${s.tradesSeen} 条 | 跟踪 ${s.symbolsTracked} 个标的 | " +
              s"基线就绪 ${s.symbolsReady}/${s.symbolsTracked} | 累计报警 ${s.alertsEmitted}"
          )
      }

      // 阻塞到停机: 中断信号或组件失败都会唤醒它, 停完全部组件 (onStop 逐个跑到) 核心最后退出
      engine.awaitShutdown()
    }
