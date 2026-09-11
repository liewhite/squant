package strategy.monitor

import hft.actor.Actor
import hft.dashboard.DashboardActor
import hft.domain.{AccountId, Instrument}
import hft.engine.Engine
import hft.event.Topics
import hft.exchange.{AccountMonitor, MarketFeed, TradingClient}
import hft.exchange.binance.{BinanceClient, BinanceCredentials, BinanceMarketFeed}
import hft.exchange.bybit.{BybitClient, BybitCredentials, BybitMarketFeed}
import hft.exchange.okx.{OkxClient, OkxCredentials, OkxMarketFeed}
import org.slf4j.LoggerFactory
import ox.supervised
import sttp.client4.DefaultSyncBackend

/** **只读实盘看板** —— 把配置里那几家交易所的账户接上，在一个页面上看仓位、挂单、净值、余额，
  * 以及（可选）它们的盘口与跨所比价。
  *
  * 运行：`sbt "runMain strategy.monitor.LiveDashboardLauncher [conf/dashboard.json]"`
  * 模板见 `conf/dashboard.example.json`。**配置含 API 密钥 -> chmod 600 且勿入库。**
  *
  * ## 只读是结构保证
  *
  * 每家装的是 [[AccountMonitor]]，不是柜台。它不注册任何命令处理能力，所以**下单指令在这个
  * 进程里连路由都没有** —— 不依赖"这里没装策略所以没人会发"这种关于当前装配的话。
  *
  * 为什么不用柜台（它也能产出这些读数）：柜台是为**执行**设计的，拿它来观察会撞上它自己的
  * 三处前提 —— 要求每个标的都有合约规格（账上一张股票永续就能让只读进程启动失败）、
  * 只管对齐过的标的（启动后新开的仓位永远不显示）、注册下单能力。详见 [[AccountMonitor]]。
  *
  * 密钥仍建议用交易所侧的**只读密钥**：那是另一道、也是更外的一道保证。
  *
  * ## 一个进程只做一件事
  *
  * 这个进程不装柜台，也**不应该**与同账户的柜台共处一个进程 —— 两者都发 `Topics.Position`，
  * 会 last-write-wins。理由见 [[AccountMonitor]] 的"不能与同账户的柜台共处一个进程"。
  */
@main def LiveDashboardLauncher(args: String*): Unit =
  val logger = LoggerFactory.getLogger("LiveDashboardLauncher")
  val confPath = args.headOption.getOrElse("conf/dashboard.json")
  val conf = DashboardConfig.load(confPath) match
    case Right(c) => c
    case Left(e)  => logger.error(s"加载配置失败: $e"); sys.exit(1)
  // 校验在任何连接之前跑完 —— 配置错误该在启动时大声失败, 而不是连上两家之后才发现第三家不对
  val venues =
    try conf.validated
    catch
      case e: IllegalArgumentException => logger.error(s"配置不合法 ($confPath): ${e.getMessage}"); sys.exit(1)

  logger.warn(s"只读实盘看板: ${venues.map(_.exchange).mkString(", ")} | 配置=$confPath")
  logger.warn("*** 只读: 本进程不注册下单能力 (结构保证)。密钥仍建议用交易所侧的只读密钥 ***")

  supervised:
    val backend = DefaultSyncBackend()

    /** 每家的 (配置, 交易客户端, 公共行情源) —— 三家的构造不同, 装配形状相同。 */
    val wired: Vector[(VenueConfig, TradingClient, MarketFeed)] = venues.map {
      case v @ VenueConfig.Binance(key, secret, _, _, _) =>
        (v, BinanceClient.trading(backend, BinanceCredentials(key, secret)), BinanceMarketFeed(backend))
      case v @ VenueConfig.Okx(key, secret, passphrase, _, _, _, quote) =>
        val credentials = OkxCredentials(key, secret, passphrase)
        // quote 留空就不传 —— 让客户端用自己的默认值, 不在这里抄第二份
        val client = quote.fold(OkxClient.trading(backend, credentials))(OkxClient.trading(backend, credentials, _))
        // 行情源与账户轮询共用**同一个**客户端实例: 规格表在实例上 (见 hft.exchange.MetaTable),
        // 两个实例就是两张表、两次拉取。行情源收的是公共客户端, 而交易客户端是它的子类。
        (v, client, OkxMarketFeed(client, backend))
      case v @ VenueConfig.Bybit(key, secret, _, _, _, accountType) =>
        val credentials = BybitCredentials(key, secret)
        val client = accountType.fold(BybitClient.trading(backend, credentials))(BybitClient.trading(backend, credentials, _))
        (v, client, BybitMarketFeed(backend))
    }

    val plugins: Vector[Actor] = wired.flatMap { (v, client, marketFeed) =>
      val monitor = AccountMonitor(client, AccountId.Live, v.symbols, v.pollMs)
      // 不看盘口的那几家就不装行情源 —— 少一条 WS 连接
      if v.watchMarket then Vector(monitor, marketFeed) else Vector(monitor)
    }

    // 装配失败最常见的原因就是密钥填错、权限没开。而那条真正的原因会埋在几十行栈里:
    // ox 的作用域异常把它挂在 suppressed 上, 打印时还可能变成 CIRCULAR REFERENCE。
    // **不吞** —— 只是先把根因用一行人话打出来, 再原样抛出去, 栈与退出码都保留。
    try
      Engine.run(plugins = plugins) { engine =>
        engine.install(DashboardActor(conf.port, conf.host))
        wired.foreach { (v, _, _) =>
          if v.watchMarket && v.symbols.nonEmpty then
            engine.watchMarket(v.symbols.map(Instrument.perp(v.exchange, _)), Set(Topics.Bbo))
        }
        logger.warn(s"看板: http://${conf.host}:${conf.port}  (Ctrl+C 退出)")
        engine.awaitShutdown()
      }
    catch
      case e: Throwable =>
        logger.error(s"看板启动失败: ${rootCauseChain(e).mkString(" <- ")}")
        throw e

/** 异常链上**去重后**的消息, 从外到内。
  *
  * 走 cause 链与 suppressed (ox 的作用域异常把真正的原因挂在 suppressed 上), 深度设上限
  * 避免自引用死循环 —— 这也是打印时出现 CIRCULAR REFERENCE 的那个原因。
  *
  * 只是**转述**内层的那些外层消息会被丢掉: 包装异常的 `getMessage` 常常就是
  * `java.lang.IllegalStateException: <内层原话>`, 留着它等于把同一句话说两遍,
  * 而内层那句已经在里面了。 */
private[monitor] def rootCauseChain(t: Throwable): Vector[String] =
  val seen = scala.collection.mutable.LinkedHashSet.empty[String]
  def walk(e: Throwable, depth: Int): Unit =
    if e != null && depth < 8 then
      Option(e.getMessage).filter(_.nonEmpty).foreach(seen.add)
      walk(e.getCause, depth + 1)
      e.getSuppressed.foreach(walk(_, depth + 1))
  walk(t, 0)
  val all = seen.toVector
  all.filterNot(m => all.exists(other => other != m && m.contains(other)))
