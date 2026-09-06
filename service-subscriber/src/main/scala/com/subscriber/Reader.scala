package com.subscriber

import org.apache.pekko.actor.{Actor, ActorLogging, ActorSystem, PoisonPill, Props, Timers}
import scala.concurrent.duration._

case object Read

// The read-load factors as the benchmark sweeps them, read once here rather than per backend, so a
// second backend cannot reinterpret what READS_PER_SEC means
object ReadConfig {
  // Aggregate across every reader, not per reader, which is what leaves READ_POOL_SIZE a pure
  // concurrency setting. 0 means no readers at all -- no actors, no connections, no timers.
  val readsPerSec: Int = sys.env.getOrElse("READS_PER_SEC", "0").toInt

  // One connection each, so DB_POOL_SIZE + READ_POOL_SIZE is the total while reads are on.
  // Deliberately independent of PUBLISHER_COUNT so the group's connection count cannot drift if the
  // anchor's publisher count changes.
  val readers: Int = sys.env.getOrElse("READ_POOL_SIZE", "4").toInt

  val enabled: Boolean = readsPerSec > 0

  // Each reader's share of the aggregate rate, as a period. Only read when enabled.
  // Mirrors the formula in service_subscriber_reader.erl.
  def periodMs: Long = math.round(1000.0 * readers / readsPerSec)
}

// The per-reader resources and read operation a backend supplies to ReaderActor. One instance is
// owned by one actor for its lifetime, so implementations need no synchronisation of their own
trait ReadTarget {
  // Runs one read to completion and discards the result. Throwing marks the read failed, so it is
  // logged and left uncounted rather than holding the read rate up while queries are erroring out.
  def read(): Unit

  def close(): Unit
}

// Drives one ReadTarget at its share of READS_PER_SEC. Self-driving: unlike BatchWriterActor nothing
// is routed here, so the pool needs no round-robin
// Runs on blocking-io-dispatcher so the target's synchronous JDBC call does not stall the default fork-join pool
// Mirrors service_subscriber_reader.erl -- keep the pacing formula in sync
class ReaderActor(periodMs: Long, mkTarget: () => ReadTarget)
    extends Actor with ActorLogging with Timers {

  // Built here rather than passed in so the target's connection is opened on this actor's own
  // blocking-io-dispatcher thread, not on whichever thread constructed the pool.
  private val target: ReadTarget = mkTarget()

  // The first read fires immediately; every later one is armed against nextDueNs.
  private var nextDueNs: Long = System.nanoTime()
  self ! Read

  // Runs one read, records it, and arms the next against a fixed deadline that advances by exactly
  // one period per cycle. Sleeping period-minus-query-time instead let per-cycle overhead outside the
  // measured read accumulate, and it differed enough between the arms to make them run different read
  // loads at the same READS_PER_SEC; audit.md's read-group section has the measurements. Mirrors the
  // pacing in service_subscriber_reader.erl.
  //
  // math.max keeps the deadline out of the past, so a reader that cannot keep up runs flat out
  // instead of burning off a debt in a burst. With the next read armed only from a completed one, at
  // most one is ever in flight, and an unreachable target shows up as an achieved rate below the
  // configured one -- which is why reports must read subscriber_reads_total and never the env var.
  override def receive: Receive = {
    case Read =>
      // A duration, not a wire timestamp, so this reads the monotonic clock rather than
      // Clock.nowMicros(); Clock is for stamps that cross the wire.
      val startNs = System.nanoTime()
      val ok =
        try { target.read(); true }
        catch {
          case e: Exception =>
            log.error(s"Read failed: ${e.getMessage}")
            false
        }
      val elapsedUs = (System.nanoTime() - startNs) / 1000

      // Left uncounted for the same reason a failed batch is never counted as committed: counting
      // it would hold the read rate up precisely when reads start failing.
      if (ok) Metrics.recordRead(elapsedUs)

      val now = System.nanoTime()
      nextDueNs = math.max(nextDueNs + periodMs * 1000000L, now)
      timers.startSingleTimer("read", Read, ((nextDueNs - now) / 1000000L).millis)
  }

  override def postStop(): Unit = target.close()
}

// A pool of self-driving ReaderActors. No round-robin counter, unlike BatchWriterPool: readers pace
// themselves rather than serving traffic from the ingest path
class ReaderPool(periodMs: Long, readers: Int, mkTarget: () => ReadTarget)(
    implicit system: ActorSystem) {

  private val actors = (0 until readers).map { _ =>
    system.actorOf(
      Props(new ReaderActor(periodMs, mkTarget))
        .withDispatcher("pekko.actor.blocking-io-dispatcher"))
  }.toVector

  // PoisonPill rather than stop, so each reader reaches postStop and closes its target
  def close(): Unit = actors.foreach(_ ! PoisonPill)
}
