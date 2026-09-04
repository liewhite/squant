package demo

import hft.actor.{Actor, ActorContext}
import hft.dashboard.DashboardActor
import hft.domain.*
import hft.engine.Engine
import hft.event.{Event, Topics}
import org.slf4j.LoggerFactory
import ox.supervised

/** 用**合成数据**驱动看板 —— 不接任何交易所, 只为把页面跑起来看一眼。
  *
  * 造的数据里特意留了几种"不好看"的情形, 因为它们恰恰是看板要显示清楚的:
  *   - 一个标的的盘口**故意不再更新**, 于是它的年龄会一直涨 (页面转黄再转红);
  *   - 一个账户只有仓位、没有全量钱包快照, 页面应标出"未列出 ≠ 0";
  *   - 实盘与影子账户在同一标的上各有各的挂单。
  *
  * 运行: sbt "demo/runMain demo.DashboardDemoLauncher [端口]"
  */
@main def DashboardDemoLauncher(args: String*): Unit =
  val logger = LoggerFactory.getLogger("DashboardDemoLauncher")
  val port = args.headOption.map(_.toInt).getOrElse(8123)
  supervised:
    Engine.run(plugins = Vector.empty) { engine =>
      engine.install(DashboardActor(port))
      engine.install(SyntheticFeed())
      logger.warn(s"看板: http://127.0.0.1:$port  (Ctrl+C 退出)")
      engine.awaitShutdown()
    }

/** 合成行情/回报源。只服务于上面那个启动器, 不参与任何真实交易路径。 */
private final class SyntheticFeed extends Actor:
  private val ex = Exchange.Binance
  private val okx = Exchange.Okx

  override def name: String = "synthetic-feed"

  private var live = 0.35
  private var paper = -0.12

  override def onStart(ctx: ActorContext): Unit =
    // 一次性的账户读数
    ctx.publish(Event.local(Topics.AccountInfo, AccountInfo(AccountId.Live, ex, 12_345.67)))
    ctx.publish(Event.local(Topics.Wallet, Wallet(AccountId.Live, ex, Map("USDT" -> 12_000.0, "BTC" -> 0.05), nowMs)))
    // OKX 这个账户**故意不发全量钱包** —— 页面应显示"未收到全量钱包快照, 未列出 ≠ 0"
    ctx.publish(Event.local(Topics.AccountInfo, AccountInfo(AccountId.Live, okx, 3_000.0)))
    ctx.publish(Event.local(Topics.Balance, Balance(AccountId.Live, okx, "USDT", 3_000.0, nowMs)))

    // 挂单: 实盘与影子各一张, 同一个标的
    ctx.publish(Event.local(Topics.OrderUpdate, OrderUpdate(
      AccountId.Live, "o-1", Some("c-1"), ex, "BTCUSDT", Side.Long, OrderStatus.Pending,
      Price(94_000.0), Coin(0.01), Coin.Zero, reduceOnly = false, nowMs)))
    ctx.publish(Event.local(Topics.OrderUpdate, OrderUpdate(
      AccountId.Paper(1), "o-2", Some("c-2"), ex, "BTCUSDT", Side.Short, OrderStatus.Pending,
      Price(96_500.0), Coin(0.02), Coin.Zero, reduceOnly = true, nowMs)))

    // 一个**再也不更新**的盘口: 用来看年龄怎么涨、页面怎么变色
    ctx.publish(Event.local(Topics.Bbo, BBO(okx, "ETH-USDT-SWAP", Price(3_100.0), Coin(5.0), Price(3_100.4), Coin(4.0), nowMs)))

    // 启动对齐会推一次初始仓位 —— 真实柜台的形态就是这样 (见 TradingGateway.syncEvents)
    ctx.publish(Event.local(Topics.Position, Position(AccountId.Live, ex, "BTCUSDT", Coin(live))))
    ctx.publish(Event.local(Topics.Position, Position(AccountId.Paper(1), ex, "BTCUSDT", Coin(paper))))

    ctx.fork {
      var i = 0
      while !ctx.sleepUnlessStopped(500) do
        i += 1
        val drift = math.sin(i / 12.0) * 400
        val mid = 95_000.0 + drift
        ctx.publish(Event.local(Topics.Bbo, BBO(ex, "BTCUSDT", Price(mid - 2.5), Coin(1.2), Price(mid + 2.5), Coin(0.8), nowMs)))
        ctx.publish(Event.local(Topics.MarkPrice, MarkPrice(ex, "BTCUSDT", Price(mid), nowMs)))
        if i % 4 == 0 then
          ctx.publish(Event.local(Topics.FundingRate, FundingRate(ex, "BTCUSDT", 0.0001 * math.cos(i / 20.0), nowMs + 3_600_000, nowMs)))
        // **仓位只在"成交"时发** —— 与真实柜台一致 (柜台在成交入账与启动对齐时才推)。
        // 从前这里每 500ms 重发一次, 页面上仓位的年龄永远是几百毫秒, 把"变更驱动读数的年龄
        // 会一直涨"这个真实形态整个盖住了 —— 于是"每个不动的仓位 30s 后必然标红"这个 bug
        // 在演示里根本看不出来。演示的形态不真实, 拿它做的验证就是假的。
        if i % 40 == 0 then
          live += 0.01
          paper -= 0.01
          ctx.publish(Event.local(Topics.Fill, Fill(AccountId.Live, ex, "BTCUSDT", Side.Long, Price(mid), Coin(0.01), nowMs)))
          ctx.publish(Event.local(Topics.Position, Position(AccountId.Live, ex, "BTCUSDT", Coin(live))))
          ctx.publish(Event.local(Topics.Position, Position(AccountId.Paper(1), ex, "BTCUSDT", Coin(paper))))
    }
    ()
