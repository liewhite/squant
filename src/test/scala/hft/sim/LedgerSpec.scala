package hft.sim

import hft.domain.*
import hft.TestUnits.given

/** 账本与撮合判定的纯单测：无线程、无延迟、无 sleep。 */
class LedgerSpec extends munit.FunSuite:
  private val ex = Exchange.Binance
  private val sym = Instrument.perp(Exchange.Binance, "BTCUSDT")
  private def empty = Ledger.empty(AccountId.Live, 10_000.0)
  private def sizeOf(l: Ledger): Double = l.positions.get(sym).map(_.size.value).getOrElse(0.0)
  private def entryOf(l: Ledger): Double = l.positions.get(sym).map(_.entryPrice.value).getOrElse(0.0)

  test("新开多头: 记录均价, 现金不变"):
    val l = empty.applyFill(sym, Side.Long, price = 100.0, qty = 2.0)
    assertEquals(sizeOf(l), 2.0)
    assertEquals(entryOf(l), 100.0)
    assertEquals(l.cash, 10_000.0)

  test("同向加仓: 加权平均成本"):
    val l = empty
      .applyFill(sym, Side.Long, 100.0, Coin(2.0))
      .applyFill(sym, Side.Long, 110.0, Coin(2.0))
    assertEquals(sizeOf(l), 4.0)
    assertEquals(entryOf(l), 105.0) // (2*100 + 2*110)/4
    assertEquals(l.cash, 10_000.0)

  test("部分平仓: 实现盈亏入现金, 均价不变"):
    val l = empty
      .applyFill(sym, Side.Long, 100.0, Coin(4.0))
      .applyFill(sym, Side.Short, 120.0, Coin(1.0)) // 平 1 个, 盈利 20
    assertEquals(sizeOf(l), 3.0)
    assertEquals(entryOf(l), 100.0)
    assertEquals(l.cash, 10_020.0)

  test("全平多头: 仓位归零, 均价清零, 盈亏入账"):
    val l = empty
      .applyFill(sym, Side.Long, 100.0, Coin(2.0))
      .applyFill(sym, Side.Short, 90.0, Coin(2.0)) // 平 2 个, 亏损 20
    assertEquals(sizeOf(l), 0.0)
    assertEquals(entryOf(l), 0.0)
    assertEquals(l.cash, 9_980.0)

  test("反手: 平掉原仓 + 剩余在成交价反向重开"):
    val l = empty
      .applyFill(sym, Side.Long, 100.0, Coin(2.0))
      .applyFill(sym, Side.Short, 120.0, Coin(5.0)) // 平多 2 (盈利 40), 反手开空 3 @120
    assertEquals(sizeOf(l), -3.0)
    assertEquals(entryOf(l), 120.0)
    assertEquals(l.cash, 10_040.0)

  test("空头平仓盈亏方向正确 (跌价获利)"):
    val l = empty
      .applyFill(sym, Side.Short, 100.0, Coin(2.0))
      .applyFill(sym, Side.Long, 90.0, Coin(2.0)) // 空头平于低价, 盈利 20
    assertEquals(sizeOf(l), 0.0)
    assertEquals(l.cash, 10_020.0)

  test("equity 用未实现盈亏估值"):
    val l = empty.applyFill(sym, Side.Long, 100.0, Coin(2.0))
    val markOf = (_: Instrument) => Some(Price(150.0))
    assertEquals(l.equity(markOf), 10_000.0 + (150.0 - 100.0) * 2.0) // 10100
    // openPositions 是**总线形态**, 只有数量 —— 未实现盈亏由 equity 表达 (见 Position 的说明)
    assertEquals(l.openPositions.head.size.value, 2.0)

  test("持有仓位却拿不到估值价 -> 抛错, 不把那段盈亏记成 0"):
    // 净值是策略杠杆闸门读的数。记 0 会让它读到一个偏小的净值且没有任何症状。
    val l = empty.applyFill(sym, Side.Long, 100.0, Coin(2.0))
    val e = intercept[RuntimeException](l.equity((_: Instrument) => None))
    assert(e.getMessage.contains("没有可用的估值价"), e.getMessage)
    // 0 价同样不是估值价 (它是一个合法的价格取值, 不能用来表示"没有")
    intercept[RuntimeException](l.equity((_: Instrument) => Some(Price(0.0))))

  test("空仓不需要估值价 —— 那一段盈亏本来就是 0"):
    assertEquals(empty.equity((_: Instrument) => None), 10_000.0)

  test("Matcher.marketable (taker, 乐观): 价格重合即可成交"):
    val bbo = BBO(ex, sym.symbol, bidPrice = 100.0, Coin(1), askPrice = 101.0, Coin(1), timestamp = 0)
    assert(!Matcher.marketable(Side.Long, 99.0, bbo))  // 出价 99 < ask 101, 够不着
    assert(Matcher.marketable(Side.Long, 101.0, bbo))  // 出价 == ask, 重合即成交
    assert(Matcher.marketable(Side.Long, 102.0, bbo))  // 出价高过 ask, 成交
    assert(!Matcher.marketable(Side.Short, 102.0, bbo)) // 要价 102 > bid 100, 够不着
    assert(Matcher.marketable(Side.Short, 100.0, bbo))  // 要价 == bid, 重合即成交
    assert(Matcher.marketable(Side.Short, 99.0, bbo))   // 要价低于 bid, 成交

  test("Matcher.crossedByBbo (maker, 悲观): 必须严格穿越, 仅触及不成交"):
    val bbo = BBO(ex, sym.symbol, bidPrice = 100.0, Coin(1), askPrice = 101.0, Coin(1), timestamp = 0)
    assert(!Matcher.crossedByBbo(Side.Long, 101.0, bbo)) // ask 恰好触及挂单价 -> 排队未消化, 不成交
    assert(Matcher.crossedByBbo(Side.Long, 101.5, bbo))  // ask 101 跌破挂单价 101.5 -> 成交
    assert(!Matcher.crossedByBbo(Side.Short, 100.0, bbo)) // bid 恰好触及挂单价 -> 不成交
    assert(Matcher.crossedByBbo(Side.Short, 99.5, bbo))   // bid 100 升破挂单价 99.5 -> 成交

  test("Matcher.crossedByTrade (maker, 悲观): 逐笔与 BBO 同一穿越口径"):
    assert(!Matcher.crossedByTrade(Side.Long, 100.0, 100.0)) // 成交价仅触及挂单价 -> 不成交
    assert(Matcher.crossedByTrade(Side.Long, 100.0, 99.9))   // 成交价跌破 -> 成交
    assert(!Matcher.crossedByTrade(Side.Short, 100.0, 100.0))
    assert(Matcher.crossedByTrade(Side.Short, 100.0, 100.1)) // 成交价升破 -> 成交

  test("maker 悲观 / taker 乐观的有意间隙: ask 恰等于挂单价时, 到达单吃单成交、簿上单不成交"):
    val bbo = BBO(ex, sym.symbol, bidPrice = 100.0, Coin(1), askPrice = 101.0, Coin(1), timestamp = 0)
    assert(Matcher.marketable(Side.Long, 101.0, bbo))     // 此刻到达 -> 主动吃单
    assert(!Matcher.crossedByBbo(Side.Long, 101.0, bbo))  // 早已在簿 -> 被动排队, 不成交

  test("Matcher.touchPrice: 买单吃卖价, 卖单吃买价"):
    val bbo = BBO(ex, sym.symbol, 100.0, Coin(1), 101.0, Coin(1), 0)
    assertEquals(Matcher.touchPrice(Side.Long, bbo).value, 101.0)
    assertEquals(Matcher.touchPrice(Side.Short, bbo).value, 100.0)

  test("同一 symbol 的不同品种是两条独立仓位 —— 按 symbol 记账会把它们合并"):
    // 期权与永续在有些交易所共享 symbol 前缀。按 symbol 记账的话, 一条期权空头与一条
    // 永续多头会合并成一个净额 —— 那个数没有意义, 两者的合约乘数与敞口含义都不同。
    val perp = Instrument.perp(Exchange.Okx, "ETH")
    val option = Instrument.option(Exchange.Okx, "ETH")
    val l = Ledger
      .empty(AccountId.Live, 10_000.0)
      .applyFill(perp, Side.Long, Price(100.0), Coin(2.0))
      .applyFill(option, Side.Short, Price(100.0), Coin(3.0))

    assertEqualsDouble(l.positions(perp).size.value, 2.0, 1e-12)
    assertEqualsDouble(l.positions(option).size.value, -3.0, 1e-12)
    assertEquals(l.positions.size, 2, "两个品种各记一条, 不合并")

  test("币本位与现货: 账本的公式对它们不成立, 入口即拒 —— 不给一个看着正常的数"):
    // 币本位的盈亏是 面值_usd x (1/开仓价 - 1/平仓价) 记在基础币上, 与 qty x 价差 量纲都不同;
    // 现货买入当场扣现金、不能做空, 而本账本的现金只在平仓时动。
    // 算出来的数看着都合理 —— 这正是它必须在入口失败的理由。
    val inverse = Instrument(Exchange.Okx, "ETH", InstrumentKind.InversePerp)
    val spot = Instrument(Exchange.Okx, "ETH", InstrumentKind.Spot)
    Vector(inverse, spot).foreach { i =>
      val e = intercept[IllegalArgumentException](
        Ledger.empty(AccountId.Live, 0.0).applyFill(i, Side.Long, Price(100.0), Coin(1.0))
      )
      assert(e.getMessage.contains("线性结算"), e.getMessage)
    }

  test("openPositions 报账本里的全部仓位 —— 不按交易所过滤"):
    // 从前它收一个 exchange 参数、只报那个所的仓位, 而 equity 对全账本求和 —— 同一个账本
    // 两个口径。更要紧的是那道过滤在替调用方防一件由路由保证不会发生的事 (柜台只收得到
    // 自己那个 (账户, 所) 的成交), 而防的方式是静默丢弃: 真出现异所仓位时它不报错。
    val binance = Instrument.perp(Exchange.Binance, "BTCUSDT")
    val okx = Instrument.perp(Exchange.Okx, "BTC")
    val l = Ledger
      .empty(AccountId.Live, 0.0)
      .applyFill(binance, Side.Long, Price(100.0), Coin(1.0))
      .applyFill(okx, Side.Long, Price(100.0), Coin(2.0))
    assertEquals(l.openPositions.map(_.instrument).toSet, Set(binance, okx))
    assertEquals(l.openPositions.map(_.exchange).toSet, Set(Exchange.Binance, Exchange.Okx), "交易所取自标的本身")
