package hft.exchange

import org.slf4j.LoggerFactory
import ox.{Ox, fork, supervised}
import ox.channels.Channel
import sttp.client4.*
import sttp.client4.ws.SyncWebSocket
import sttp.client4.ws.sync.*
import sttp.ws.WebSocketFrame

/** 服务端关闭连接 (收到 Close 帧)，触发重连 */
private final class WsServerClosedException(reason: String) extends RuntimeException(reason)

/** 通用 WebSocket 重连循环。
  *
  * 基于 sttp 同步 WebSocket，每条连接占用两个虚拟线程：
  *   - 发送线程: 消费 outgoing channel，是连接的唯一写入者 (Pong 响应也经由该 channel)
  *   - 接收线程: 阻塞读取帧，文本帧回调 onText
  *
  * 任一线程出错即拆除整条连接，退避后重连，重连成功先回调 onConnect (用于恢复订阅)。
  */
object WsLoop:
  private val logger = LoggerFactory.getLogger(getClass)

  /** 在当前作用域 fork 一个常驻的自动重连 WS 循环。
    *
    * @param name             连接名 (日志用)
    * @param url              每次 (重) 连接时求值，可在其中完成动态准备 (如获取 listenKey)
    * @param outgoing         出站帧 channel；重连后未发出的帧不补发，由 onConnect 恢复订阅状态
    * @param onText           收到文本帧的回调，在接收线程上执行
    * @param onConnect        连接建立后、收发开始前的回调，向 outgoing 投递恢复订阅帧
    * @param reconnectDelayMs 重连退避间隔
    */
  def run(
      name: String,
      backend: WebSocketSyncBackend,
      url: () => String,
      outgoing: Channel[WebSocketFrame],
      onText: String => Unit,
      onConnect: () => Unit,
      reconnectDelayMs: Long = 3000,
  )(using Ox): Unit =
    fork {
      while true do
        try
          val target = url()
          logger.info(s"[$name] connecting: $target")
          basicRequest
            .get(uri"$target")
            .response(asWebSocketAlways(pump(name, _, outgoing, onText, onConnect)))
            .send(backend)
        catch
          case e: InterruptedException => throw e // 作用域取消，正常退出
          case e: Exception =>
            logger.warn(s"[$name] connection lost: ${e.getMessage}, reconnecting in ${reconnectDelayMs}ms")
        Thread.sleep(reconnectDelayMs)
    }
    ()

  /** 单条连接的收发泵，连接断开时以异常退出 */
  private def pump(
      name: String,
      ws: SyncWebSocket,
      outgoing: Channel[WebSocketFrame],
      onText: String => Unit,
      onConnect: () => Unit,
  ): Unit =
    logger.info(s"[$name] connected")
    onConnect()
    supervised:
      // 发送线程: 连接的唯一写入者
      fork:
        while true do ws.send(outgoing.receive())
      // 接收循环 (当前线程)；文本消息可能分片到达，重组到 finalFragment 后回调
      val fragments = StringBuilder()
      while true do
        ws.receive() match
          case WebSocketFrame.Text(payload, finalFragment, _) =>
            if finalFragment && fragments.isEmpty then onText(payload)
            else
              fragments.append(payload)
              if finalFragment then
                onText(fragments.result())
                fragments.clear()
          case WebSocketFrame.Ping(payload)       => outgoing.send(WebSocketFrame.Pong(payload))
          case _: WebSocketFrame.Pong             => ()
          case _: WebSocketFrame.Binary           => ()
          case WebSocketFrame.Close(code, reason) =>
            throw WsServerClosedException(s"server closed: code=$code reason=$reason")
