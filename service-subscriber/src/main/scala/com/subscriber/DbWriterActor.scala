package com.subscriber

import org.apache.pekko.actor.{Actor, ActorLogging}
import org.apache.pekko.actor.Timers
import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._

case class InsertRow(deviceName: String, value: Int,
                     publisherEpochUs: Long, subscriberReceiveUs: Long)
case class InsertStatus(deviceName: String, status: String)
case object Flush

// Owns one JDBC connection and one row buffer; flushes as a single multi-row INSERT on batchSize or timeout
// Runs on blocking-io-dispatcher so synchronous JDBC calls do not stall the default fork-join pool
class DbWriterActor(batchSize: Int, timeoutMs: Long) extends Actor with ActorLogging with Timers {

  private val conn: Connection = {
    val url  = sys.env.getOrElse("DB_URL",      "jdbc:postgresql://timescaledb:5432/epu")
    val props = new Properties()
    props.setProperty("user",     sys.env.getOrElse("DB_USER",     "postgres"))
    props.setProperty("password", sys.env.getOrElse("DB_PASSWORD", "postgres"))
    // Server-side prepare from the first execution instead of pgjdbc's default fifth, so the batch
    // statement is parsed once like the Erlang arm's, which epgsql parses at init.
    props.setProperty("prepareThreshold", "1")
    DriverManager.getConnection(url, props)
  }

  // Array parameters keep the SQL text fixed at any batch size, so this is prepared once instead of
  // rebuilt per flush as a VALUES list was. Mirrors db_backend_timescaledb.erl -- keep the two in sync.
  private val batchStmt: PreparedStatement = conn.prepareStatement(
    "INSERT INTO Data (DeviceName, Value, Timestamp) " +
    "SELECT unnest(?::text[]), unnest(?::int4[]), unnest(?::timestamptz[])")

  // Status rows share this writer's single connection rather than a pool of their own, so
  // DB_POOL_SIZE is the total connection count in both arms; see TimescaleDBBackend.
  private val statusStmt: PreparedStatement = conn.prepareStatement(
    "INSERT INTO sensor_status (DeviceName, Status) VALUES (?, ?)")

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
    // connection reproduces the Erlang arm, where a status write can queue ahead of a data write.
    case s: InsertStatus =>
      try {
        statusStmt.setString(1, s.deviceName)
        statusStmt.setString(2, s.status)
        statusStmt.executeUpdate()
      } catch {
        case e: Exception => log.error(s"Status insert failed: ${e.getMessage}")
      }
  }

  // Executes a single multi-row INSERT for all buffered rows and records latency per row at ack time
  private def flush(): Unit = {
    if (buffer.isEmpty) return
    val rows = buffer.toList
    buffer.clear()
    timers.cancel("flush")

    try {
      val names = rows.map(_.deviceName).toArray
      val values = rows.map(r => Integer.valueOf(r.value)).toArray
      // The reading's own instant, rebuilt from the epoch value the row carries -- the raw payload
      // string is not forwarded, so this is the only conversion on the write path.
      val stamps = rows.map(r => Clock.toOffsetDateTime(r.publisherEpochUs)).toArray

      batchStmt.setArray(1, conn.createArrayOf("text",        names.asInstanceOf[Array[Object]]))
      batchStmt.setArray(2, conn.createArrayOf("int4",        values.asInstanceOf[Array[Object]]))
      batchStmt.setArray(3, conn.createArrayOf("timestamptz", stamps.asInstanceOf[Array[Object]]))
      batchStmt.executeUpdate()
      // Only after a successful executeUpdate — a failure skips this via catch, so a failed batch
      // is never counted as committed.
      Metrics.recordCommitBatch(rows)(_.publisherEpochUs, _.subscriberReceiveUs)
    } catch {
      case e: Exception =>
        log.error(s"Batch insert failed: ${e.getMessage}")
    }
  }

  // Drains any remaining buffered rows before the actor is terminated and releases the JDBC connection
  override def postStop(): Unit = {
    if (buffer.nonEmpty) flush()
    batchStmt.close()
    statusStmt.close()
    conn.close()
  }
}
