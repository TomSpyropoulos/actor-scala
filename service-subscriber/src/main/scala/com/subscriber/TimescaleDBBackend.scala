package com.subscriber

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.time.OffsetDateTime

import scala.concurrent.{ExecutionContext, Future}

/** TimescaleDB (PostgreSQL) backend using HikariCP for connection pooling.
  *
  * A pool of up to 20 JDBC connections is created at construction time.
  * Each insert acquires a connection from the pool, executes a prepared
  * statement, and returns the connection. Futures run on the caller's
  * ExecutionContext, which should be a blocking-IO-aware dispatcher to avoid
  * stalling the default fork-join pool (see application.conf).
  *
  * Connection parameters are read from environment variables:
  *   DB_URL      (default: jdbc:postgresql://timescaledb:5432/epu)
  *   DB_USER     (default: postgres)
  *   DB_PASSWORD (default: postgres)
  */
class TimescaleDBBackend extends DatabaseBackend {
  private val config = new HikariConfig()
  config.setJdbcUrl(
    sys.env.getOrElse("DB_URL", "jdbc:postgresql://timescaledb:5432/epu")
  )
  config.setUsername(sys.env.getOrElse("DB_USER", "postgres"))
  config.setPassword(sys.env.getOrElse("DB_PASSWORD", "postgres"))
  config.addDataSourceProperty("cachePrepStmts", "true")
  config.addDataSourceProperty("prepStmtCacheSize", "250")
  config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048")
  config.setMaximumPoolSize(20)

  private val dataSource = new HikariDataSource(config)

  def insertData(deviceName: String, value: Int, timestamp: String)(
      implicit ec: ExecutionContext): Future[Unit] = Future {
    val conn = dataSource.getConnection
    try {
      val stmt = conn.prepareStatement(
        "INSERT INTO Data (DeviceName, Value, Timestamp) VALUES (?, ?, ?)"
      )
      stmt.setString(1, deviceName)
      stmt.setInt(2, value)
      stmt.setObject(3, OffsetDateTime.parse(timestamp))
      stmt.executeUpdate()
      ()
    } finally {
      conn.close()
    }
  }

  def insertStatus(deviceName: String, status: String)(
      implicit ec: ExecutionContext): Future[Unit] = Future {
    val conn = dataSource.getConnection
    try {
      val stmt = conn.prepareStatement(
        "INSERT INTO sensor_status (DeviceName, Status) VALUES (?, ?)"
      )
      stmt.setString(1, deviceName)
      stmt.setString(2, status)
      stmt.executeUpdate()
      ()
    } finally {
      conn.close()
    }
  }

  def close(): Unit = dataSource.close()
}
