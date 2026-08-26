package hft.exchange

import org.slf4j.LoggerFactory
import ox.{fork, supervised}
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

  /** 起一条连接泵线程。连接在线程内建立，url 求值失败同样令作用域终止。
    *
    * 要的是"起一条线程"的能力而不是并发作用域本身：插件 (见 [[MarketFeed]]) 给的就是
    * 它自己那条 fork，本对象因此不需要知道作用域是谁的、嵌在哪一层。
    *
    * @param url           连接时求值，可在其中完成动态准备 (如获取 listenKey)
    * @param outgoing      出站帧 channel；连接建立前入队的帧会在建立后立即发送
    * @param onText        收到完整文本消息的回调，在接收线程上执行，抛出的异常向上传播
    * @param spawn         起一条常驻线程 (抛出的异常级联终止引擎)
    * @param idleTimeoutMs 空闲超时: 超过该时长未收到任何帧即抛错。TCP 半开连接不产生
    *                      任何错误，是"无错误可抛"的静默停滞——watchdog 把它转化为可抛的
    *                      错误。Binance 服务端每 ~3 分钟 ping 一次，活连接必有帧
    */
  def run(
      name: String,
      backend: WebSocketSyncBackend,
      url: () => String,
      outgoing: Channel[WebSocketFrame],
      onText: String => Unit,
      spawn: (=> Unit) => Unit,
      idleTimeoutMs: Long = 5 * 60 * 1000,
  ): Unit =
    spawn {
      val target = url()
      logger.info(s"[$name] connecting: $target")
      basicRequest
        .get(uri"$target")
        .response(asWebSocketAlways(pump(name, _, outgoing, onText, idleTimeoutMs)))
        .send(backend)
      // pump 是无限循环，只能以异常退出；走到这里说明连接以未建模的方式结束
      throw IllegalStateException(s"[$name] websocket terminated unexpectedly")
    }

  /** 单条连接的收发泵，连接断开/空闲超时时以异常退出 */
  private def pump(
      name: String,
      ws: SyncWebSocket,
      outgoing: Channel[WebSocketFrame],
      onText: String => Unit,
      idleTimeoutMs: Long,
  ): Unit =
    logger.info(s"[$name] connected")
    val lastFrameNanos = java.util.concurrent.atomic.AtomicLong(System.nanoTime())
    supervised:
      // 发送线程: 连接的唯一写入者
      fork:
        while true do ws.send(outgoing.receive())
      // 空闲 watchdog: 接收线程阻塞在 receive 上无法自察，由旁路线程检测
      fork:
        while true do
          Thread.sleep(idleTimeoutMs / 4)
          val idleMs = (System.nanoTime() - lastFrameNanos.get()) / 1_000_000
          if idleMs > idleTimeoutMs then
            throw IllegalStateException(s"[$name] no frame received for ${idleMs}ms, connection presumed dead")
      // 接收循环 (当前线程)；文本消息可能分片到达，重组到 finalFragment 后回调
      val fragments = StringBuilder()
      while true do
        val frame = ws.receive()
        lastFrameNanos.set(System.nanoTime())
        frame match
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
