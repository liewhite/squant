package hft.dashboard

import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec
import com.github.plokhotnyuk.jsoniter_scala.macros.{CodecMakerConfig, JsonCodecMaker}
import hft.domain.*

/** 看板的**线格式** —— 与领域类型分开的一层 DTO。
  *
  * 分开的两个理由:
  *   - 领域类型用 opaque type 表达单位 ([[Coin]] / [[Price]] / [[Rate]])。那是**进程内**的
  *     防线, 出到 JSON 只能是数字, 由这一层显式解包 —— 解包点集中在这里, 而不是散在
  *     各个 codec 的隐式推导里。
  *   - 页面要的字段与领域字段不是同一组: 页面要的是"这个数多久没更新了", 而领域里存的是
  *     "它什么时候到的"。年龄在**读取时刻**才算得出来, 所以它是这一层的字段。
  *
  * 每个可能没有的读数都是 `Option` 且带 `ageMs`: 一个 4 分钟前的盘口和一个刚到的盘口
  * 在页面上必须长得不一样, 见 [[Stamped]]。
  */
final case class Reading[A](value: A, ageMs: Long)

final case class BboView(bid: Double, ask: Double, mid: Double, spread: Double, bidQty: Double, askQty: Double)
final case class FundingView(rate: Double, nextSettleMs: Timestamp)
final case class OrderView(
    orderId: String,
    clientOrderId: Option[String],
    side: String,
    status: String,
    price: Double,
    quantity: Double,
    filledQuantity: Double,
    reduceOnly: Boolean,
    ageMs: Long,
)
final case class FillView(side: String, price: Double, size: Double, ageMs: Long)

final case class AccountRowView(
    account: String,
    position: Option[Reading[Double]],
    pendingOrders: Vector[OrderView],
    lastFill: Option[FillView],
)

final case class SymbolRowView(
    exchange: String,
    symbol: String,
    bbo: Option[Reading[BboView]],
    markPrice: Option[Reading[Double]],
    funding: Option[Reading[FundingView]],
    lastTradePrice: Option[Reading[Double]],
    accounts: Vector[AccountRowView],
)

final case class BalanceView(currency: String, amount: Double, ageMs: Long)

final case class AccountSummaryView(
    account: String,
    exchange: String,
    equity: Option[Reading[Double]],
    balances: Vector[BalanceView],
    /** false = **还没收到全量钱包快照**, 此时"某币不在下面的表里"不代表余额是 0。 */
    walletKnown: Boolean,
)

/** 看板一次读取的全部内容。
  *
  * @param generatedAtMs 生成这份视图的本地时刻 —— 所有 `ageMs` 都是相对它算的
  * @param eventsApplied 已折叠的事件条数。**0 表示总线上一条事件都没来过**, 与
  *                      "来过但内容都是空的"是两回事: 前者说明看板没接上 (订阅漏了、
  *                      引擎还没起、行情源没连上), 后者说明确实无事发生
  * @param lastEventAgeMs 距最后一条事件多久 —— 看板整体的心跳。长时间不动 = 数据流断了
  */
final case class BoardView(
    generatedAtMs: Timestamp,
    eventsApplied: Long,
    lastEventAgeMs: Option[Long],
    symbols: Vector[SymbolRowView],
    accounts: Vector[AccountSummaryView],
)

object BoardView:
  /** `transientNone` / `transientEmpty` 都关掉: 默认配置会把 `None` 与空集合**整个字段省掉**,
    * 于是页面拿到的是 `undefined` 而不是 `null` / `[]` —— "这个字段不存在"与"这个读数没有"
    * 在 JS 那边又变成同一件事, 而分开它们正是这份视图存在的理由。
    * 顺带一提, 空数组被省掉会让页面的 `rows.length` 直接抛 TypeError。 */
  given JsonValueCodec[BoardView] =
    JsonCodecMaker.make(CodecMakerConfig.withTransientNone(false).withTransientEmpty(false))

  // Schema 显式派生: 自动派生对泛型的 Reading[A] 推不出来, 而报错只说"找不到 Vector[SymbolRowView]
  // 的 Schema", 定位不到真正缺的那一个。写出来一次, 之后加字段的报错就指得准。
  given sttp.tapir.Schema[BboView] = sttp.tapir.Schema.derived
  given sttp.tapir.Schema[FundingView] = sttp.tapir.Schema.derived
  given sttp.tapir.Schema[OrderView] = sttp.tapir.Schema.derived
  given sttp.tapir.Schema[FillView] = sttp.tapir.Schema.derived
  given readingSchema[A: sttp.tapir.Schema]: sttp.tapir.Schema[Reading[A]] = sttp.tapir.Schema.derived
  given sttp.tapir.Schema[AccountRowView] = sttp.tapir.Schema.derived
  given sttp.tapir.Schema[SymbolRowView] = sttp.tapir.Schema.derived
  given sttp.tapir.Schema[BalanceView] = sttp.tapir.Schema.derived
  given sttp.tapir.Schema[AccountSummaryView] = sttp.tapir.Schema.derived
  given sttp.tapir.Schema[BoardView] = sttp.tapir.Schema.derived

  /** 把快照投影成视图。**纯函数** —— `now` 由调用方给, 不读墙钟, 于是可单测。 */
  def of(snapshot: BoardSnapshot, now: Timestamp): BoardView =
    BoardView(
      generatedAtMs = now,
      eventsApplied = snapshot.eventsApplied,
      lastEventAgeMs = snapshot.lastEventAt.map(now - _),
      symbols = snapshot.symbols.toVector.sortBy(kv => (kv._1.exchange.toString, kv._1.symbol)).map(kv => symbolRow(kv._2, now)),
      accounts = snapshot.accounts.toVector
        .sortBy(kv => (kv._1.account.toString, kv._1.exchange.toString))
        .map((key, s) => summaryRow(key, s, now)),
    )

  private def reading[A, B](s: Option[Stamped[A]], now: Timestamp)(f: A => B): Option[Reading[B]] =
    s.map(st => Reading(f(st.value), st.ageMs(now)))

  private def symbolRow(b: SymbolBoard, now: Timestamp): SymbolRowView =
    SymbolRowView(
      exchange = b.instrument.exchange.toString,
      symbol = b.instrument.symbol,
      bbo = reading(b.bbo, now)(q =>
        BboView(q.bidPrice.value, q.askPrice.value, q.midPrice.value, q.spread.value, q.bidQty.value, q.askQty.value)
      ),
      markPrice = reading(b.mark, now)(_.price.value),
      funding = reading(b.funding, now)(f => FundingView(f.rate, f.nextSettleTime)),
      lastTradePrice = reading(b.lastTrade, now)(_.price.value),
      accounts = b.accounts.toVector.sortBy(_._1.toString).map((id, acc) => accountRow(id, acc, now)),
    )

  private def accountRow(id: AccountId, acc: AccountBoard, now: Timestamp): AccountRowView =
    AccountRowView(
      account = id.toString,
      position = reading(acc.position, now)(_.value),
      pendingOrders = acc.pendingOrders.toVector.sortBy(_._1).map((_, st) => orderView(st, now)),
      lastFill = acc.lastFill.map(st =>
        FillView(st.value.side.toString, st.value.price.value, st.value.size.value, st.ageMs(now))
      ),
    )

  private def orderView(st: Stamped[OrderUpdate], now: Timestamp): OrderView =
    val o = st.value
    OrderView(
      orderId = o.orderId,
      clientOrderId = o.clientOrderId,
      side = o.side.toString,
      status = o.status.toString,
      price = o.price.value,
      quantity = o.quantity.value,
      filledQuantity = o.filledQuantity.value,
      reduceOnly = o.reduceOnly,
      ageMs = st.ageMs(now),
    )

  private def summaryRow(key: AccountExchange, s: AccountSummary, now: Timestamp): AccountSummaryView =
    AccountSummaryView(
      account = key.account.toString,
      exchange = key.exchange.toString,
      equity = reading(s.equity, now)(identity),
      balances = s.stampedBalances.toVector.sortBy(_._1).map((ccy, st) => BalanceView(ccy, st.value, st.ageMs(now))),
      walletKnown = s.wallet.known,
    )
