package com.subscriber

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource

import org.apache.pekko.actor.ActorSystem

import scala.concurrent.{ExecutionContext, Future}

// The JDBC scheme and connection options this backend's driver needs. Named here rather than in
// DbConfig because both belong to the driver, and read by Database.readTarget so the read and write
// paths build the same URL.
object MySQLBackend {
  val Driver = "mysql"

  // connectionTimeZone=UTC is load-bearing: MySQL DATETIME stores no offset, so Connector/J would
  // otherwise shift every bound timestamp by the JVM's zone. That breaks the read group silently and
  // leaves the latency metrics looking correct, which is why the MySQL smoke-test appendix checks the
  // read window. allowPublicKeyRetrieval/useSSL cover caching_sha2_password over the in-compose link.
  val UrlParams = "?connectionTimeZone=UTC&allowPublicKeyRetrieval=true&useSSL=false"
}

// Concrete DatabaseBackend for MySQL: non-batching uses HikariCP with per-Future latency recording;
// batching delegates to the shared BatchWriterPool, supplying only a MySQLBatchTarget
class MySQLBackend(implicit system: ActorSystem) extends DatabaseBackend {

  // Read through DbConfig rather than the environment directly, so this backend and the read target
  // it shares a database with can never be configured apart.
  private val dbUrl  = DbConfig.urlFor(MySQLBackend.Driver, MySQLBackend.UrlParams)
  private val dbUser = DbConfig.user
  private val dbPass = DbConfig.password

  // useServerPrepStmts/cachePrepStmts are Connector/J's counterpart of the prepareThreshold pgjdbc
  // takes in TimescaleDBBackend: prepared server-side and kept, not re-parsed per execution.
  private def mkPool(size: Int): HikariDataSource = {
    val config = new HikariConfig()
    config.setJdbcUrl(dbUrl)
    config.setUsername(dbUser)
    config.setPassword(dbPass)
    config.addDataSourceProperty("useServerPrepStmts", "true")
    config.addDataSourceProperty("cachePrepStmts",     "true")
    config.setMaximumPoolSize(size)
    new HikariDataSource(config)
  }

  // --- Non-batching: HikariCP pool for data inserts ---
  private val dataSource: Option[HikariDataSource] =
    if (!BatchConfig.enabled) Some(mkPool(BatchConfig.writers)) else None

  // --- Batching: shared writer pool, backed by this backend's JDBC target ---
  // BatchConfig.size goes to the target as well as the pool: a VALUES list has fixed arity, so the
  // target needs the size to build its full-size statement.
  private val writers: Option[BatchWriterPool] =
    if (BatchConfig.enabled)
      Some(new BatchWriterPool(BatchConfig.size, BatchConfig.timeoutMs, BatchConfig.writers,
                               () => new MySQLBatchTarget(dbUrl, dbUser, dbPass, BatchConfig.size)))
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
            stmt.setObject(3, Clock.toLocalDateTimeUtc(publisherEpochUs))
            stmt.executeUpdate()
            // Metrics owns what a commit means, but the call site is per-backend on this side, so a
            // backend that skipped this would look healthy and be silently non-comparable.
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
