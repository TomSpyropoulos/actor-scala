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

  // connectionTimeZone=UTC is load-bearing, not tidiness: MySQL DATETIME stores no offset, so
  // Connector/J would otherwise shift every bound timestamp by the JVM's zone. That would leave the
  // latency metrics correct, since those are computed in the subscriber from the epoch value and
  // never read back, while silently breaking the read group -- its bounded window would match no
  // rows. allowPublicKeyRetrieval/useSSL cover MySQL 8's caching_sha2_password over the plaintext
  // in-compose link.
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

  // useServerPrepStmts/cachePrepStmts are what Connector/J actually reads; they are the MySQL
  // counterpart of the prepareThreshold pgjdbc takes in TimescaleDBBackend, and mean the same
  // thing -- prepared server-side and kept, rather than re-parsed per execution.
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
  // BatchConfig.size is handed to the target as well as to the pool: the target needs it to build
  // its full-size multi-row statement, since a VALUES list has a fixed arity where an array
  // parameter does not.
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
            // Recorded here, on the same line of the same path as TimescaleDBBackend does it: Metrics
            // owns what a commit means, but the call site is per-backend on this side, so a backend
            // that skipped it would produce a run that looked healthy and was silently non-comparable.
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
