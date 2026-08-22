package hft.engine

import hft.actor.{Actor, ActorContext}
import hft.domain.*
import hft.event.{Event, Topics}
import hft.exchange.ExchangeClient
import org.slf4j.LoggerFactory

/** 账户净值的周期刷新。
  *
  * 净值随行情持续变动却没有对应的 WS 推送，只能周期性 REST 拉取。风控 (杠杆闸门) 直接
  * 拿它决策，所以它的**新鲜度本身是正确性问题**：注意读到的净值最多滞后一个刷新周期，
  * 临界阈值应自留余量。
  *
  * 未配置凭证的交易所在首次拉取后退出轮询 —— 没有账户就没有净值要刷新，这是确定安全的例外。
  */
final class AccountRefresher(clients: Iterable[ExchangeClient], intervalMs: Long) extends Actor:
  private val logger = LoggerFactory.getLogger(classOf[AccountRefresher])

  override def name: String = "account-refresher"

  override def onStart(ctx: ActorContext): Unit =
    ctx.fork {
      var polled = clients.toVector
      while polled.nonEmpty && !ctx.sleepUnlessStopped(intervalMs) do
        polled = polled.filter(client => AccountRefresher.publishAccountInfo(client, ctx.publish, logger))
    }

object AccountRefresher:
  /** 拉一次账户信息并发布。返回 false 表示该交易所未配置凭证 (无需再轮询)；其余失败致命。
    *
    * 时间戳由 [[Event.local]] 在**响应到手之后**盖 —— 盖在发起请求之前会把整段 REST 往返
    * 算进事件年龄，把"对端慢"误诊成"本机处理不过来"。
    */
  private[engine] def publishAccountInfo(
      client: ExchangeClient,
      publish: Event[Exchange, AccountInfo] => Unit,
      logger: org.slf4j.Logger,
  ): Boolean =
    client.fetchAccountInfo() match
      case Right(info) =>
        publish(Event.local(Topics.AccountInfo, info))
        true
      case Left(ExchangeError.Auth(_)) =>
        logger.info(s"No credentials for ${client.exchange}, skipping account info")
        false
      case Left(e) =>
        throw IllegalStateException(s"Failed to fetch account info from ${client.exchange}: ${e.message}")
