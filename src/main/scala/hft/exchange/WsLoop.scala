package hft.exchange

import org.slf4j.LoggerFactory
import ox.{Ox, fork, supervised}
import ox.channels.Channel
import sttp.client4.*
import sttp.client4.ws.SyncWebSocket
import sttp.client4.ws.sync.*
import sttp.ws.WebSocketFrame

/** 服务端关闭连接 (收到 Close 帧) */
private final class WsServerClosedException(reason: String) extends RuntimeException(reason)

/** 通用 WebSocket 连接泵 —— fail-fast，不做重连。
  *
  * 错误处理哲学：私有流断线期间的推送无法回放，任何"重连恢复"都会造成静默的状态发散；
  * 因此连接异常 (建连失败/服务端关闭/发送失败/消息解析失败) 一律向上传播，令 fork 失败、
  * 整个引擎作用域级联终止，由进程重启后的启动对齐保证状态正确。
  *
  * 每条连接占用两个虚拟线程：
  *   - 发送线程: 消费 outgoing channel，是连接的唯一写入者 (Pong 响应也经由该 channel)
  *   - 接收线程: 阻塞读取帧，重组分片后将文本回调 onText
  */
object WsLoop:
  private val logger = LoggerFactory.getLogger(getClass)

  /** 在当前作用域 fork 连接泵。连接在 fork 内建立，url 求值失败同样令作用域终止。
    *
    * @param url      连接时求值，可在其中完成动态准备 (如获取 listenKey)
    * @param outgoing 出站帧 channel；连接建立前入队的帧会在建立后立即发送
    * @param onText   收到完整文本消息的回调，在接收线程上执行，抛出的异常向上传播
    */
  def run(
      name: String,
      backend: WebSocketSyncBackend,
      url: () => String,
      outgoing: Channel[WebSocketFrame],
      onText: String => Unit,
  )(using Ox): Unit =
    fork {
      val target = url()
      logger.info(s"[$name] connecting: $target")
      basicRequest
        .get(uri"$target")
        .response(asWebSocketAlways(pump(name, _, outgoing, onText)))
        .send(backend)
      // pump 是无限循环，只能以异常退出；走到这里说明连接以未建模的方式结束
      throw IllegalStateException(s"[$name] websocket terminated unexpectedly")
    }
    ()

  /** 单条连接的收发泵，连接断开时以异常退出 */
  private def pump(
      name: String,
      ws: SyncWebSocket,
      outgoing: Channel[WebSocketFrame],
      onText: String => Unit,
  ): Unit =
    logger.info(s"[$name] connected")
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
            throw WsServerClosedException(s"[$name] server closed: code=$code reason=$reason")
