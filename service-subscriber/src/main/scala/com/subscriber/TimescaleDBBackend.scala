package com.subscriber

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.util.concurrent.atomic.AtomicInteger

import org.apache.pekko.actor.{ActorSystem, Props, PoisonPill}

import scala.concurrent.{ExecutionContext, Future}

// Concrete DatabaseBackend for TimescaleDB: non-batching uses HikariCP with per-Future latency recording;
// batching mode routes rows round-robin to a pool of DbWriterActors that flush on batchSize or timeout
class TimescaleDBBackend(implicit system: ActorSystem) extends DatabaseBackend {

  private val batchEnabled = sys.env.getOrElse("BATCH_ENABLED",    "false").toBoolean
  private val batchSize    = sys.env.getOrElse("BATCH_SIZE",       "100").toInt
  private val timeoutMs    = sys.env.getOrElse("BATCH_TIMEOUT_MS", "1000").toLong
  private val poolSize     = sys.env.getOrElse("DB_POOL_SIZE",     "20").toInt

  private val dbUrl  = sys.env.getOrElse("DB_URL",      "jdbc:postgresql://timescaledb:5432/epu")
  private val dbUser = sys.env.getOrElse("DB_USER",     "postgres")
  private val dbPass = sys.env.getOrElse("DB_PASSWORD", "postgres")

  // Builds a HikariCP pool with prepared-statement caching enabled
  private def mkPool(size: Int): HikariDataSource = {
    val config = new HikariConfig()
    config.setJdbcUrl(dbUrl)
    config.setUsername(dbUser)
    config.setPassword(dbPass)
    config.addDataSourceProperty("cachePrepStmts",        "true")
    config.addDataSourceProperty("prepStmtCacheSize",     "250")
    config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048")
    config.setMaximumPoolSize(size)
    new HikariDataSource(config)
  }

  // --- Non-batching: HikariCP pool for data inserts ---
  private val dataSource: Option[HikariDataSource] =
    if (!batchEnabled) Some(mkPool(poolSize)) else None

  // --- Batching: pool of DbWriterActors ---
  private val writers = if (batchEnabled) {
    (0 until poolSize).map { _ =>
      system.actorOf(
        Props(new DbWriterActor(batchSize, timeoutMs))
          .withDispatcher("pekko.actor.blocking-io-dispatcher"))
    }.toVector
  } else Vector.empty

  // Status and data share one counter, mirroring the single next_index() the Erlang dispatcher uses
  // for both, so neither arm gives status writes a routing path of their own.
  private val counter = new AtomicInteger(0)

  private def nextWriter(): Int = math.abs(counter.getAndIncrement() % poolSize)

  // Routes to DbWriterActor (batch) or executes inline via HikariCP (non-batch); records latency after DB ack
  def insertData(deviceName: String, value: Int,
                 publisherEpochUs: Long, subscriberReceiveUs: Long)(
      implicit ec: ExecutionContext): Future[Unit] = {
    if (batchEnabled) {
      writers(nextWriter()) ! InsertRow(deviceName, value, publisherEpochUs, subscriberReceiveUs)
      Future.successful(())
    } else {
      Future {
        val conn = dataSource.get.getConnection
        try {
          val stmt = conn.prepareStatement(
            "INSERT INTO Data (DeviceName, Value, Timestamp) VALUES (?, ?, ?)")
          stmt.setString(1, deviceName)
          stmt.setInt(2, value)
          stmt.setObject(3, Clock.toOffsetDateTime(publisherEpochUs))
          stmt.executeUpdate()
          stmt.close()
          Metrics.recordCommit(publisherEpochUs, subscriberReceiveUs)
        } finally {
          conn.close()
        }
      }
    }
  }

  // Routed over the same connections as data rather than a pool of its own, so DB_POOL_SIZE is the
  // total connection count here as it already is in Erlang, where insert_status shares the worker pool.
  def insertStatus(deviceName: String, status: String)(
      implicit ec: ExecutionContext): Future[Unit] = {
    if (batchEnabled) {
      writers(nextWriter()) ! InsertStatus(deviceName, status)
      Future.successful(())
    } else Future {
      val conn = dataSource.get.getConnection
      try {
        val stmt = conn.prepareStatement(
          "INSERT INTO sensor_status (DeviceName, Status) VALUES (?, ?)")
        stmt.setString(1, deviceName)
        stmt.setString(2, status)
        stmt.executeUpdate()
        stmt.close()
      } finally {
        conn.close()
      }
    }
  }

  // Gracefully stops all DbWriterActors (letting them drain) and closes the HikariCP pool
  def close(): Unit = {
    writers.foreach(_ ! PoisonPill)
    dataSource.foreach(_.close())
  }
}
