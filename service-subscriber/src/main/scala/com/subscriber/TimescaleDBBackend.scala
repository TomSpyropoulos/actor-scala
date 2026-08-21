package com.subscriber

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.time.OffsetDateTime
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

  // Status inserts always go through a small dedicated pool (low volume).
  private val statusPool: HikariDataSource = mkPool(2)

  // --- Batching: pool of DbWriterActors ---
  private val writers = if (batchEnabled) {
    (0 until poolSize).map { _ =>
      system.actorOf(
        Props(new DbWriterActor(batchSize, timeoutMs))
          .withDispatcher("pekko.actor.blocking-io-dispatcher"))
    }.toVector
  } else Vector.empty

  private val counter = new AtomicInteger(0)

  // Routes to DbWriterActor (batch) or executes inline via HikariCP (non-batch); records latency after DB ack
  def insertData(deviceName: String, value: Int, timestamp: String,
                 publisherEpochUs: Long, subscriberReceiveUs: Long)(
      implicit ec: ExecutionContext): Future[Unit] = {
    if (batchEnabled) {
      val idx = math.abs(counter.getAndIncrement() % poolSize)
      writers(idx) ! InsertRow(deviceName, value, timestamp, publisherEpochUs, subscriberReceiveUs)
      Future.successful(())
    } else {
      Future {
        val conn = dataSource.get.getConnection
        try {
          val stmt = conn.prepareStatement(
            "INSERT INTO Data (DeviceName, Value, Timestamp) VALUES (?, ?, ?)")
          stmt.setString(1, deviceName)
          stmt.setInt(2, value)
          stmt.setObject(3, OffsetDateTime.parse(timestamp))
          stmt.executeUpdate()
          stmt.close()
          Metrics.committedCount.inc()   // 1 row committed
          val ackUs = Clock.nowMicros()
          Metrics.e2eLatency.observe(math.max(0L, ackUs - publisherEpochUs) / 1000.0)
          Metrics.dbWriteLatency.observe(math.max(0L, ackUs - subscriberReceiveUs) / 1000.0)
        } finally {
          conn.close()
        }
      }
    }
  }

  // Writes a sensor liveness status row using the dedicated small status pool
  def insertStatus(deviceName: String, status: String)(
      implicit ec: ExecutionContext): Future[Unit] = Future {
    val conn = statusPool.getConnection
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

  // Gracefully stops all DbWriterActors (letting them drain) and closes both HikariCP pools
  def close(): Unit = {
    writers.foreach(_ ! PoisonPill)
    dataSource.foreach(_.close())
    statusPool.close()
  }
}
