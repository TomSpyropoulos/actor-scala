package com.subscriber

import org.apache.pekko.actor.{Actor, ActorLogging, ActorSystem, PoisonPill, Props, Timers}
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._

case class InsertRow(deviceName: String, value: Int,
                     publisherEpochUs: Long, subscriberReceiveUs: Long)
case class InsertStatus(deviceName: String, status: String)
case object Flush

// The batching factors as the benchmark sweeps them, read once here rather than per backend, so a
// second backend cannot reinterpret what BATCH_SIZE or BATCH_TIMEOUT_MS mean
object BatchConfig {
  val enabled: Boolean = sys.env.getOrElse("BATCH_ENABLED",    "false").toBoolean
  val size: Int        = sys.env.getOrElse("BATCH_SIZE",       "100").toInt
  val timeoutMs: Long  = sys.env.getOrElse("BATCH_TIMEOUT_MS", "1000").toLong
  // DB_POOL_SIZE. Lives here rather than in a backend because it is the total PostgreSQL connection
  // count in both modes -- writer actors when batching, connection pool size when not.
  val writers: Int     = sys.env.getOrElse("DB_POOL_SIZE",     "20").toInt
}

// The per-writer resources and write operations a backend supplies to BatchWriterActor. One instance
// is owned by one actor for its lifetime, so implementations need no synchronisation of their own
trait BatchTarget {
  // Writes a whole flushed buffer. Throwing marks the batch uncommitted; returning normally is what
  // the writer treats as the DB ack, so this must not return before the write is durable.
  def writeBatch(rows: Seq[InsertRow]): Unit

  // Writes one status transition. Never buffered, so implementations should execute it immediately.
  def writeStatus(deviceName: String, status: String): Unit

  def close(): Unit
}

// Owns one row buffer and one BatchTarget; flushes on batchSize or timeout
// Runs on blocking-io-dispatcher so a target's synchronous I/O does not stall the default fork-join pool
// Mirrors the buffering in service_subscriber_db.erl -- keep the flush triggers in sync
class BatchWriterActor(batchSize: Int, timeoutMs: Long, mkTarget: () => BatchTarget)
    extends Actor with ActorLogging with Timers {

  // Built here rather than passed in so the target's connection is opened on this actor's own
  // blocking-io-dispatcher thread, not on whichever thread constructed the pool.
  private val target: BatchTarget = mkTarget()

  private val buffer = ListBuffer[InsertRow]()

  // Buffers incoming rows and triggers a flush on batchSize or arms the timeout on the first row
  override def receive: Receive = {
    case row: InsertRow =>
      buffer += row
      if (buffer.size >= batchSize) flush()
      else if (buffer.size == 1)
        timers.startSingleTimer("flush", Flush, timeoutMs.millis)

    case Flush =>
      flush()

    // Executed immediately rather than buffered: volume is transition-only, and letting it share the
    // writer reproduces the Erlang arm, where a status write can queue ahead of a data write.
    case s: InsertStatus =>
      try target.writeStatus(s.deviceName, s.status)
      catch { case e: Exception => log.error(s"Status insert failed: ${e.getMessage}") }
  }

  // Hands the buffer to the target and records the commit for every row it covered. Recording lives
  // here, not in the target, so no backend can omit it or stamp the ack at a different point.
  private def flush(): Unit = {
    if (buffer.isEmpty) return
    val rows = buffer.toList
    buffer.clear()
    timers.cancel("flush")

    try {
      target.writeBatch(rows)
      // Only after writeBatch returns — a failure skips this via catch, so a failed batch is never
      // counted as committed.
      Metrics.recordCommitBatch(rows)(_.publisherEpochUs, _.subscriberReceiveUs)
    } catch {
      case e: Exception =>
        log.error(s"Batch insert failed: ${e.getMessage}")
    }
  }

  // Drains any remaining buffered rows before the actor is terminated and releases the target
  override def postStop(): Unit = {
    if (buffer.nonEmpty) flush()
    target.close()
  }
}

// A pool of BatchWriterActors plus the round-robin that spreads writes across them
class BatchWriterPool(batchSize: Int, timeoutMs: Long, writers: Int, mkTarget: () => BatchTarget)(
    implicit system: ActorSystem) {

  private val actors = (0 until writers).map { _ =>
    system.actorOf(
      Props(new BatchWriterActor(batchSize, timeoutMs, mkTarget))
        .withDispatcher("pekko.actor.blocking-io-dispatcher"))
  }.toVector

  // Status and data share one counter, mirroring the single next_index() the Erlang dispatcher uses
  // for both, so neither arm gives status writes a routing path of their own.
  private val counter = new AtomicInteger(0)

  // Accepts InsertRow and InsertStatus; the writer decides which is buffered and which is immediate
  def route(msg: Any): Unit =
    actors(math.abs(counter.getAndIncrement() % writers)) ! msg

  // PoisonPill rather than stop, so each writer drains its buffer in postStop before closing
  def close(): Unit = actors.foreach(_ ! PoisonPill)
}
