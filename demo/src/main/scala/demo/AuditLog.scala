package demo

import scala.collection.mutable.ArrayBuffer

/** 纯领域对象：审计日志。
  *
  * 与 [[Account]] 一样不关心并发。被包成 Actor 后，来自多个账户 Actor 的
  * 并发 `record` 调用会被串行化追加，`entries` 不会出现竞态。
  */
class AuditLog:
  private val entries = ArrayBuffer.empty[String]

  def record(event: String): Unit = entries += event

  def all: List[String] = entries.toList

  def size: Int = entries.size
