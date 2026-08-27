package hft.exchange

import hft.domain.*
import hft.TestUnits.given

/** 账本、记账进度、三方对账 —— 全是纯逻辑，时间由测试给定。
  *
  * 这些行为此前只能靠"装一台柜台、连一条总线、发一串事件"来验证，而那些设施与要验证的
  * 东西毫无关系；更要命的是**时间**：墓碑保留期一分钟、stale 告警一小时，用真实时钟根本
  * 测不动。分出来之后它们变成普通的函数调用。
  */
class PositionBookSpec extends munit.FunSuite:
  private val ex = Exchange.Binance
  private val btc = "BTCUSDT"
  private val eth = "ETHUSDT"
  private val account = AccountId.Live

  /** 视作零的量级 —— 币本位。真实柜台由 SymbolMeta 换算, 这里直接给 */
  private def book(dust: Double = 1e-9) = PositionBook(account, ex, _ => dust)

  private def position(symbol: Symbol, size: Double) =
    Position(account, ex, symbol, Coin(size), Price(100.0), 0.0)

  private def pending(orderId: String, symbol: Symbol, filled: Double) =
    OrderUpdate(account, orderId, Some("c1"), ex, symbol, Side.Long, OrderStatus.PartiallyFilled(Coin(filled)),
      Price(100.0), Coin(1.0), Coin(filled), 0L)

  private def settledDelta(b: PositionBook, orderId: String, symbol: Symbol, cumulative: Double, now: Long = 0L) =
    b.settle(orderId, symbol, Side.Long, Price(100.0), Coin(cumulative), now) match
      case PositionBook.Settled.Recorded(delta, _) => Some(delta.value)
      case PositionBook.Settled.Unchanged          => None
      case PositionBook.Settled.Regressed(_, _)    => None // 同样不入账; 单独一条用例盯着它
      case PositionBook.Settled.Rejected(reason)   => fail(s"unexpected rejection: $reason")

  // ==================== 记账 ====================

  test("按累计量记增量: 分批成交只记差额"):
    val b = book()
    b.align(Set(btc), Vector.empty, Vector.empty, now = 0L)
    assertEquals(settledDelta(b, "o1", btc, 0.3), Some(0.3))
    assertEquals(settledDelta(b, "o1", btc, 0.8), Some(0.5), "记的是增量, 不是累计")
    assertEqualsDouble(b.positionOf(btc).size.value, 0.8, 1e-12)

  test("累计量没涨就什么都不发 —— 重复推送与乱序后到的那条都落在这里"):
    val b = book()
    b.align(Set(btc), Vector.empty, Vector.empty, now = 0L)
    settledDelta(b, "o1", btc, 0.5)
    assertEquals(settledDelta(b, "o1", btc, 0.5), None)
    assertEquals(settledDelta(b, "o1", btc, 0.3), None, "倒退更不该记")
    assertEqualsDouble(b.positionOf(btc).size.value, 0.5, 1e-12)

  test("明显的倒退与浮点尾巴分开报 —— 都不入账, 但前者留得下线索"):
    // 倒退绝大多数是乱序后到的旧推送 (正常), 少数是适配层读错了字段, 本地分辨不了。
    // 所以它不是告警, 但也不该跟"没有变化"混在一起 —— 排查时这是唯一的线索。
    val b = book(dust = 0.0005)
    b.align(Set(btc), Vector.empty, Vector.empty, now = 0L)
    settledDelta(b, "o1", btc, 0.8)
    b.settle("o1", btc, Side.Long, Price(100.0), Coin(0.3), 0L) match
      case PositionBook.Settled.Regressed(cumulative, already) =>
        assertEqualsDouble(cumulative.value, 0.3, 1e-12)
        assertEqualsDouble(already.value, 0.8, 1e-12)
      case other => fail(s"倒退该说得出口: $other")
    assertEquals(b.settle("o1", btc, Side.Long, Price(100.0), Coin(0.8 - 1e-9), 0L), PositionBook.Settled.Unchanged,
      "尾巴量级的回退只是浮点噪声, 不是倒退")
    assertEqualsDouble(b.positionOf(btc).size.value, 0.8, 1e-12, "两者都不入账")

  test("成交均价为零 -> 拒绝入账"):
    // 拿委托价 (市价单为空) 记账会把持仓均价记成 0, 平仓时算出巨额假亏损而毫无报错
    val b = book()
    b.align(Set(btc), Vector.empty, Vector.empty, now = 0L)
    b.settle("o1", btc, Side.Long, Price(0.0), Coin(0.5), 0L) match
      case PositionBook.Settled.Rejected(reason) => assert(reason.contains("成交均价"), reason)
      case other                                 => fail(s"应拒绝: $other")
    assertEqualsDouble(b.positionOf(btc).size.value, 0.0, 1e-12)

  test("容差以下的增量视作零 —— 浮点尾巴不产生幻影成交"):
    val b = book(dust = 0.0005) // 半个最小变动单位
    b.align(Set(btc), Vector.empty, Vector.empty, now = 0L)
    settledDelta(b, "o1", btc, 0.8)
    assertEquals(settledDelta(b, "o1", btc, 0.3 + 0.5), None, "0.8000000000000001 不是新成交")

  // ==================== 对齐 ====================

  test("对齐按标的增量做 —— 不抹掉上一批"):
    // 引擎为每批新加的策略都发一次对齐指令, symbols 只含那一批。整本替换会让先装的
    // 策略的仓位归零, 那些策略从此按零仓决策而没有任何症状。
    val b = book()
    b.align(Set(btc), Vector(position(btc, 0.5)), Vector.empty, now = 0L)
    b.align(Set(eth), Vector.empty, Vector.empty, now = 0L) // 第二批: 交易所没返回 ETH -> 零仓
    assertEqualsDouble(b.positionOf(btc).size.value, 0.5, 1e-12, "第一批的仓位必须还在")
    assert(b.manages(btc) && b.manages(eth))

  test("对齐用既有挂单的已成交量填回记账进度"):
    // 少了这一步: 拉到的仓位里本已含着那 0.3, 而记账进度是空的, 于是累计 0.5 被算成
    // 增量 0.5 而不是 0.2 —— 仓位凭空多出 0.3
    val b = book()
    b.align(Set(btc), Vector(position(btc, 0.3)), Vector(pending("o1", btc, 0.3)), now = 0L)
    assertEquals(settledDelta(b, "o1", btc, 0.5), Some(0.2))
    assertEqualsDouble(b.positionOf(btc).size.value, 0.5, 1e-12)

  test("对齐只吃本批标的的挂单 —— 账本重置与进度重置必须同批"):
    // 传进来一张别批标的的挂单: 它的记账进度若被刷成快照值, 而对应账本没跟着重置,
    // 那笔差额就永久漏记了。调用方今天恰好只拉本批, 但这条不变量得由账本自己守。
    val b = book()
    b.align(Set(btc), Vector(position(btc, 0.5)), Vector.empty, now = 0L)
    settledDelta(b, "o2", eth, 0.4) // ETH 还没纳入管辖, 但记账进度确实记下了 0.4

    b.align(Set(btc), Vector(position(btc, 0.5)), Vector(pending("o2", eth, 0.9)), now = 1L)
    assertEqualsDouble(settledDelta(b, "o2", eth, 0.6).getOrElse(fail("这笔增量该记进去")), 0.2, 1e-12,
      "ETH 的进度不该被这次 BTC 的对齐改写 —— 否则这条回报会被算成负增量而永久漏记")

  test("同一标的再次对齐: 账本与进度一起换成新快照, 增量从新基线算"):
    val b = book()
    b.align(Set(btc), Vector(position(btc, 0.3)), Vector(pending("o1", btc, 0.3)), now = 0L)
    // 这张单在两次对齐之间又成交了一些, 第二次快照里仓位与已成交量一起前进
    b.align(Set(btc), Vector(position(btc, 0.7)), Vector(pending("o1", btc, 0.7)), now = 1L)
    assertEqualsDouble(b.positionOf(btc).size.value, 0.7, 1e-12, "账本认快照")
    assertEqualsDouble(settledDelta(b, "o1", btc, 0.9).getOrElse(fail("这笔增量该记进去")), 0.2, 1e-12,
      "进度也认快照, 增量按新基线算")

  test("拒绝入账不推进记账进度 —— 下一条带真价格的回报要记全额"):
    // 价格填错时若把进度推到 0.5, 这笔成交就再也没有机会入账了。
    val b = book()
    b.align(Set(btc), Vector.empty, Vector.empty, now = 0L)
    b.settle("o1", btc, Side.Long, Price(0.0), Coin(0.5), 0L)
    assertEquals(settledDelta(b, "o1", btc, 0.5), Some(0.5), "进度没被那次拒绝推走")
    assertEqualsDouble(b.positionOf(btc).size.value, 0.5, 1e-12)

  test("对齐之前什么标的都不管 —— 那时既不知道管什么, 账本也没初值"):
    val b = book()
    assert(!b.manages(btc))

  // ==================== 记账进度的生命周期 ====================

  test("终态之后保留一分钟墓碑 —— 晚到的成交认得出'已经记过了'"):
    val b = book()
    b.align(Set(btc), Vector.empty, Vector.empty, now = 0L)
    settledDelta(b, "o1", btc, 0.5)
    b.markTerminal("o1", now = 1_000L)

    // 保留期内: 晚到的那条累计量增量为零
    b.audit(now = 1_000L + PositionBook.SettledRetentionMs)
    assertEquals(settledDelta(b, "o1", btc, 0.5, now = 1_000L), None, "保留期内不该重记")

    // 过了保留期: 进度被清掉。此后同一订单的回报会被当成新成交 —— 所以
    // AccountFeed 的契约明确写着"不得重放已投递的报告"
    b.audit(now = 1_000L + PositionBook.SettledRetentionMs + 1)
    assertEquals(settledDelta(b, "o1", btc, 0.5, now = 2_000L), Some(0.5))

  test("非终态的记账进度永远不清, 只在长期没动静时报一句"):
    // 清掉一张还活着的订单的进度, 下一条累计量会以"已记 0"重新记一遍 —— 仓位翻倍。
    // 而"部分成交之后继续挂着"是完全正常的形态 (GTC、冰山单)。
    val b = book()
    b.align(Set(btc), Vector.empty, Vector.empty, now = 0L)
    settledDelta(b, "o1", btc, 0.5)

    val long = PositionBook.StaleSettlementMs * 2
    val alarms = b.audit(now = long)
    assertEquals(
      alarms.collect { case PositionBook.Alarm.StaleSettlement(id, qty) => (id, qty.value) },
      Vector(("o1", 0.5)),
      "该报一句丢推送的嫌疑",
    )
    assert(b.audit(now = long + 1).forall(!_.isInstanceOf[PositionBook.Alarm.StaleSettlement]), "只报一次, 别刷屏")
    assertEquals(settledDelta(b, "o1", btc, 0.5, now = long), None, "进度还在, 没被清掉")

  // ==================== 三方对账 ====================

  test("连续三次对不上才喊 —— 偶尔一次是时序窗口"):
    val b = book()
    b.align(Set(btc), Vector.empty, Vector.empty, now = 0L)
    settledDelta(b, "o1", btc, 0.5)      // 订单回报账 = 0.5
    b.observeReported(btc, Coin(0.9))    // 交易所说 0.9
    // 成交明细账也记上, 免得内部比对先喊起来
    b.recordFill(btc, Side.Long, Price(100.0), Coin(0.5))

    def externalAlarms(now: Long) =
      b.audit(now).collect { case PositionBook.Alarm.Disagreement(_, v: PositionBook.Verdict.External) => v }

    assert(externalAlarms(1L).isEmpty, "第一次不喊")
    assert(externalAlarms(2L).isEmpty, "第二次不喊")
    assertEquals(externalAlarms(3L).size, 1, "第三次才喊")
    assert(externalAlarms(4L).isEmpty, "喊过就不再刷屏")

  test("对上了就把计数清零 —— 时序窗口过去之后不该留下账"):
    val b = book()
    b.align(Set(btc), Vector.empty, Vector.empty, now = 0L)
    settledDelta(b, "o1", btc, 0.5)
    b.recordFill(btc, Side.Long, Price(100.0), Coin(0.5))

    b.observeReported(btc, Coin(0.9)); b.audit(1L); b.audit(2L) // 攒了两次
    b.observeReported(btc, Coin(0.5)); b.audit(3L)              // 追上了
    b.observeReported(btc, Coin(0.9))
    assert(b.audit(4L).isEmpty, "计数已清零, 又要从头攒起")

  test("两条内部渠道对不上 -> 报的是'我们这边的 bug'"):
    // 成交明细账与订单回报账都是我们自己记的, 来源却完全不同。对不上说明少解析了
    // 一条推送 / 字段读错 / 去重去多了 —— 与账户被外部改动无关。
    val b = book()
    b.align(Set(btc), Vector.empty, Vector.empty, now = 0L)
    settledDelta(b, "o1", btc, 0.5)
    b.recordFill(btc, Side.Long, Price(100.0), Coin(0.3)) // 明细少了一笔

    val alarms = (1 to 3).flatMap(i => b.audit(i.toLong)).collect {
      case PositionBook.Alarm.Disagreement(_, v: PositionBook.Verdict.Internal) => v
    }
    assertEquals(alarms.size, 1)
    assert(alarms.head.explain.contains("我们这边的 bug"), alarms.head.explain)

  test("交易所还没推过仓位 -> 只做内部比对, 不假装外部一致"):
    val b = book()
    b.align(Set(btc), Vector.empty, Vector.empty, now = 0L)
    settledDelta(b, "o1", btc, 0.5)
    b.recordFill(btc, Side.Long, Price(100.0), Coin(0.5))
    assert((1 to 5).flatMap(i => b.audit(i.toLong)).isEmpty, "无从比较, 不该报")
