package hft.engine

import hft.actor.{Actor, ActorContext}
import hft.domain.*
import hft.event.{Event, Topics}
import hft.exchange.TradingClient
import org.slf4j.LoggerFactory

/** 账户净值的周期刷新。
  *
  * 净值随行情持续变动却没有对应的 WS 推送，只能周期性 REST 拉取。风控 (杠杆闸门) 直接
  * 拿它决策，所以它的**新鲜度本身是正确性问题**：注意读到的净值最多滞后一个刷新周期，
  * 临界阈值应自留余量。
  *
  * 只轮询有私有面的交易所（[[TradingClient]]）：没配凭证的交易所根本没有净值可刷，
  * 而"有没有凭证"是装配期就定死的事实，不该在每一轮轮询里重新发现一次。
  */
final class AccountRefresher(clients: Iterable[TradingClient], intervalMs: Long) extends Actor:
  private val logger = LoggerFactory.getLogger(classOf[AccountRefresher])

  override def name: String = "account-refresher"

  override def onStart(ctx: ActorContext): Unit =
    ctx.fork {
      val polled = clients.toVector
      while polled.nonEmpty && !ctx.sleepUnlessStopped(intervalMs) do
        polled.foreach(client => AccountRefresher.publishAccountInfo(client, ctx.publish))
    }

object AccountRefresher:
  /** 拉一次账户信息并发布。任何失败都致命 —— "没凭证"已经由类型排除在外。
    *
    * 时间戳由 [[Event.local]] 在**响应到手之后**盖 —— 盖在发起请求之前会把整段 REST 往返
    * 算进事件年龄，把"对端慢"误诊成"本机处理不过来"。
    */
  private[engine] def publishAccountInfo(
      client: TradingClient,
      publish: Event[AccountExchange, AccountInfo] => Unit,
  ): Unit =
    client.fetchAccountInfo() match
      case Right(info) => publish(Event.local(Topics.AccountInfo, info))
      case Left(e) =>
        throw IllegalStateException(s"Failed to fetch account info from ${client.exchange}: ${e.message}")
