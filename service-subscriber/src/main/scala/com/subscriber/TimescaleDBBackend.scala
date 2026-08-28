package com.subscriber

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource

import org.apache.pekko.actor.ActorSystem

import scala.concurrent.{ExecutionContext, Future}

// Concrete DatabaseBackend for TimescaleDB: non-batching uses HikariCP with per-Future latency recording;
// batching delegates to the shared BatchWriterPool, supplying only a TimescaleBatchTarget
class TimescaleDBBackend(implicit system: ActorSystem) extends DatabaseBackend {

  private val dbUrl  = sys.env.getOrElse("DB_URL",      "jdbc:postgresql://timescaledb:5432/epu")
  private val dbUser = sys.env.getOrElse("DB_USER",     "postgres")
  private val dbPass = sys.env.getOrElse("DB_PASSWORD", "postgres")

  // prepareThreshold is the knob pgjdbc actually reads: Hikari's cachePrepStmts family is a MySQL
  // convention that pgjdbc ignores entirely. 1 means server-side prepared from the first execution,
  // matching TimescaleBatchTarget and epgsql's parse-at-init.
  private def mkPool(size: Int): HikariDataSource = {
    val config = new HikariConfig()
    config.setJdbcUrl(dbUrl)
    config.setUsername(dbUser)
    config.setPassword(dbPass)
    config.addDataSourceProperty("prepareThreshold", "1")
    config.setMaximumPoolSize(size)
    new HikariDataSource(config)
  }

  // --- Non-batching: HikariCP pool for data inserts ---
  private val dataSource: Option[HikariDataSource] =
    if (!BatchConfig.enabled) Some(mkPool(BatchConfig.writers)) else None

  // --- Batching: shared writer pool, backed by this backend's JDBC target ---
  private val writers: Option[BatchWriterPool] =
    if (BatchConfig.enabled)
      Some(new BatchWriterPool(BatchConfig.size, BatchConfig.timeoutMs, BatchConfig.writers,
                               () => new TimescaleBatchTarget(dbUrl, dbUser, dbPass)))
    else None

  // Routes to the writer pool (batch) or executes inline via HikariCP (non-batch); records latency after DB ack
  def insertData(deviceName: String, value: Int,
                 publisherEpochUs: Long, subscriberReceiveUs: Long)(
      implicit ec: ExecutionContext): Future[Unit] = {
    writers match {
      case Some(pool) =>
        pool.route(InsertRow(deviceName, value, publisherEpochUs, subscriberReceiveUs))
        Future.successful(())
      case None =>
        Future {
          val conn = dataSource.get.getConnection
          var stmt: java.sql.PreparedStatement = null
          try {
            stmt = conn.prepareStatement(
              "INSERT INTO Data (DeviceName, Value, Timestamp) VALUES (?, ?, ?)")
            stmt.setString(1, deviceName)
            stmt.setInt(2, value)
            stmt.setObject(3, Clock.toOffsetDateTime(publisherEpochUs))
            stmt.executeUpdate()
            Metrics.recordCommit(publisherEpochUs, subscriberReceiveUs)
          } finally {
            // Closed here, not after executeUpdate: a throw there would otherwise return the
            // connection to the pool with its statement still open.
            if (stmt != null) stmt.close()
            conn.close()
          }
        }
    }
  }

  // Routed over the same connections as data rather than a pool of its own, so DB_POOL_SIZE is the
  // total connection count here as it already is in Erlang, where insert_status shares the worker pool.
  def insertStatus(deviceName: String, status: String)(
      implicit ec: ExecutionContext): Future[Unit] = {
    writers match {
      case Some(pool) =>
        pool.route(InsertStatus(deviceName, status))
        Future.successful(())
      case None =>
        Future {
          val conn = dataSource.get.getConnection
          var stmt: java.sql.PreparedStatement = null
          try {
            stmt = conn.prepareStatement(
              "INSERT INTO sensor_status (DeviceName, Status) VALUES (?, ?)")
            stmt.setString(1, deviceName)
            stmt.setString(2, status)
            stmt.executeUpdate()
          } finally {
            if (stmt != null) stmt.close()
            conn.close()
          }
        }
    }
  }

  // Gracefully stops all writers (letting them drain) and closes the HikariCP pool
  def close(): Unit = {
    writers.foreach(_.close())
    dataSource.foreach(_.close())
  }
}
