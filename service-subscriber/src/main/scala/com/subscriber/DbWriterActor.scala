package com.subscriber

import org.apache.pekko.actor.{Actor, ActorLogging}
import org.apache.pekko.actor.Timers
import java.sql.{Connection, DriverManager}
import java.time.OffsetDateTime
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._

case class InsertRow(deviceName: String, value: Int, timestamp: String,
                     publisherEpochMs: Long, subscriberReceiveMs: Long)
case object Flush

// Owns one JDBC connection and one row buffer; flushes as a single multi-row INSERT on batchSize or timeout
// Runs on blocking-io-dispatcher so synchronous JDBC calls do not stall the default fork-join pool
class DbWriterActor(batchSize: Int, timeoutMs: Long) extends Actor with ActorLogging with Timers {

  private val conn: Connection = {
    val url  = sys.env.getOrElse("DB_URL",      "jdbc:postgresql://timescaledb:5432/epu")
    val user = sys.env.getOrElse("DB_USER",     "postgres")
    val pass = sys.env.getOrElse("DB_PASSWORD", "postgres")
    DriverManager.getConnection(url, user, pass)
  }

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
  }

  // Executes a single multi-row INSERT for all buffered rows and records latency per row at ack time
  private def flush(): Unit = {
    if (buffer.isEmpty) return
    val rows = buffer.toList
    buffer.clear()
    timers.cancel("flush")

    val placeholders = rows.indices.map(_ => "(?, ?, ?)").mkString(", ")
    val sql  = s"INSERT INTO Data (DeviceName, Value, Timestamp) VALUES $placeholders"
    val stmt = conn.prepareStatement(sql)
    try {
      rows.zipWithIndex.foreach { case (r, i) =>
        stmt.setString(i * 3 + 1, r.deviceName)
        stmt.setInt(i * 3 + 2, r.value)
        stmt.setObject(i * 3 + 3, OffsetDateTime.parse(r.timestamp))
      }
      stmt.executeUpdate()
      // Count the whole batch as committed only after a successful executeUpdate (a failure skips this via catch)
      Metrics.committedCount.inc(rows.size.toDouble)
      val ackMs = System.currentTimeMillis()
      rows.foreach { r =>
        Metrics.e2eLatency.observe(math.max(0, ackMs - r.publisherEpochMs))
        Metrics.dbWriteLatency.observe(math.max(0, ackMs - r.subscriberReceiveMs))
      }
    } catch {
      case e: Exception =>
        log.error(s"Batch insert failed: ${e.getMessage}")
    } finally {
      stmt.close()
    }
  }

  // Drains any remaining buffered rows before the actor is terminated and releases the JDBC connection
  override def postStop(): Unit = {
    if (buffer.nonEmpty) flush()
    conn.close()
  }
}
