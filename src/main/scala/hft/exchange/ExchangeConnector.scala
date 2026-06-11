package hft.exchange

import hft.domain.Exchange
import hft.messaging.{EventBus, IncomeEvent}
import ox.Ox

/** 交易所行情/账户推送连接器，封装 WebSocket 交互。
  *
  * 实现负责：
  *   - 维护到交易所的长连接，将原始推送解析为统一的 [[IncomeEvent]] 发布到 income 总线
  *   - 若配置了凭证，自动建立私有流 (订单回报、仓位、余额)
  *
  * Fail-fast 契约：私有流断线期间的推送无法回放，"重连恢复"会造成静默的状态发散。
  * 因此连接断开、解析失败、不理解的消息一律抛出异常终止引擎作用域，
  * 由进程重启后的启动对齐保证状态正确。
  */
trait ExchangeConnector:
  def exchange: Exchange

  /** 启动连接器，在给定并发作用域内 fork 出常驻虚拟线程。
    * 解析出的事件发布到 incomeBus
    */
  def start(incomeBus: EventBus[IncomeEvent])(using Ox): Unit

  /** 订阅公共行情。可在任意时刻调用 (须在 start 之后) */
  def subscribe(kinds: Set[SubscriptionKind]): Unit
