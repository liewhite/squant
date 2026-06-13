package hft.exchange

import hft.domain.Exchange
import hft.messaging.{EventBus, IncomeEvent}
import ox.Ox

/** 公共行情流：订阅并将交易所市场数据 (BBO/标记价格/资金费率/指数价格) 解析为
  * 统一的 [[IncomeEvent]] 发布到 income 总线。
  *
  * 与 [[AccountStream]] 拆开的意义：公共行情是无凭证、可独立运行的数据源，模拟盘
  * 复用真实公共行情、用虚拟柜台替换账户流，即可在实盘行情下跑纸面交易。
  *
  * Fail-fast 契约：连接断开、解析失败、不理解的消息一律抛出异常终止引擎作用域，
  * 由进程重启后的启动对齐保证状态正确。
  */
trait MarketDataStream:
  def exchange: Exchange

  /** 启动行情连接，在给定并发作用域内 fork 出常驻虚拟线程，事件发布到 incomeBus */
  def start(incomeBus: EventBus[IncomeEvent])(using Ox): Unit

  /** 订阅公共行情。可在任意时刻调用 (须在 start 之后) */
  def subscribe(kinds: Set[SubscriptionKind]): Unit

/** 私有账户流：推送订单回报、成交、仓位、余额等账户数据到 income 总线。
  *
  * 仅在配置了凭证 (实盘) 或使用虚拟柜台 (模拟盘) 时存在。无账户即不订阅，
  * 框架退化为纯公共行情研究模式。
  *
  * Fail-fast 契约同 [[MarketDataStream]]：私有流断线期间的推送无法回放，
  * "重连恢复"会造成静默的状态发散，故一律抛异常终止。
  */
trait AccountStream:
  def exchange: Exchange

  /** 启动账户流，在给定并发作用域内 fork 出常驻虚拟线程，事件发布到 incomeBus */
  def start(incomeBus: EventBus[IncomeEvent])(using Ox): Unit
