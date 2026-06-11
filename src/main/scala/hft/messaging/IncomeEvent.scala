package hft.messaging

import hft.domain.*

/** 事件数据类型 */
enum EventData:
  case FundingRateUpdate(rate: FundingRate)
  case BboUpdate(bbo: BBO)
  case MarkPriceUpdate(markPrice: MarkPrice)
  case IndexPriceUpdate(indexPrice: IndexPrice)
  case PositionUpdate(position: Position)
  case OrderUpdated(update: OrderUpdate)
  /** 成交事件 (用于乐观更新仓位) */
  case FillUpdate(fill: Fill)
  case BalanceUpdate(balance: Balance)
  /** 账户信息 (净值 + 总持仓名义价值) */
  case AccountInfoUpdate(exchange: Exchange, info: AccountInfo)
  /** 时钟事件 (用于超时检测等定时任务) */
  case Clock

/** 统一的交易所事件
  *
  * @param exchangeTs 交易所推送的时间戳
  * @param localTs    本地接收时间戳
  */
final case class IncomeEvent(
    exchangeTs: Timestamp,
    localTs: Timestamp,
    data: EventData,
):
  /** 事件关联的 Symbol；账户级事件与 Clock 返回 None */
  def symbol: Option[Symbol] = data match
    case EventData.FundingRateUpdate(r) => Some(r.symbol)
    case EventData.BboUpdate(b)         => Some(b.symbol)
    case EventData.MarkPriceUpdate(m)   => Some(m.symbol)
    case EventData.IndexPriceUpdate(i)  => Some(i.symbol)
    case EventData.PositionUpdate(p)    => Some(p.symbol)
    case EventData.OrderUpdated(u)      => Some(u.symbol)
    case EventData.FillUpdate(f)        => Some(f.symbol)
    case EventData.BalanceUpdate(_)     => None
    case EventData.AccountInfoUpdate(_, _) | EventData.Clock => None

  /** 事件来源交易所 */
  def exchange: Option[Exchange] = data match
    case EventData.FundingRateUpdate(r)  => Some(r.exchange)
    case EventData.BboUpdate(b)          => Some(b.exchange)
    case EventData.MarkPriceUpdate(m)    => Some(m.exchange)
    case EventData.IndexPriceUpdate(i)   => Some(i.exchange)
    case EventData.PositionUpdate(p)     => Some(p.exchange)
    case EventData.OrderUpdated(u)       => Some(u.exchange)
    case EventData.FillUpdate(f)         => Some(f.exchange)
    case EventData.BalanceUpdate(b)      => Some(b.exchange)
    case EventData.AccountInfoUpdate(e, _) => Some(e)
    case EventData.Clock                 => None

  /** 路由键: Some 表示按 (exchange, symbol) 定向路由，None 表示广播给所有策略 */
  def routing: Option[(Exchange, Symbol)] =
    for
      e <- exchange
      s <- symbol
    yield (e, s)

object IncomeEvent:
  /** 以本地时间为交易所时间构造事件 (本地产生的事件没有交易所时间戳) */
  def local(data: EventData): IncomeEvent =
    val ts = nowMs
    IncomeEvent(ts, ts, data)

  /** 以交易所时间戳构造事件 */
  def at(exchangeTs: Timestamp, data: EventData): IncomeEvent =
    IncomeEvent(exchangeTs, nowMs, data)
