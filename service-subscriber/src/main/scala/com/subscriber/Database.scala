package com.subscriber

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.time.OffsetDateTime

import scala.concurrent.{ExecutionContext, Future}

/** Database handler for TimescaleDB using HikariCP for connection pooling. */
object Database {
  private val config = new HikariConfig()
  config.setJdbcUrl(
    sys.env.getOrElse("DB_URL", "jdbc:postgresql://localhost:5432/epu")
  )
  config.setUsername(sys.env.getOrElse("DB_USER", "postgres"))
  config.setPassword(sys.env.getOrElse("DB_PASSWORD", "postgres"))
  config.addDataSourceProperty("cachePrepStmts", "true")
  config.addDataSourceProperty("prepStmtCacheSize", "250")
  config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048")
  // Optimize connection pool size for blocking I/O dispatcher
  config.setMaximumPoolSize(20)

  val dataSource = new HikariDataSource(config)

  /** Inserts a sensor reading into the Data table asynchronously.
    * @param deviceName
    *   The name of the device sending the data.
    * @param value
    *   The sensor reading value.
    * @param timestamp
    *   The ISO-8601 timestamp of the reading.
    * @param ec
    *   Execution context (should be a dedicated dispatcher for blocking I/O).
    */
  def insertData(deviceName: String, value: Int, timestamp: String)(implicit ec: ExecutionContext): Future[Unit] = Future {
    val conn = dataSource.getConnection
    try {
      val stmt = conn.prepareStatement(
        "INSERT INTO Data (DeviceName, Value, Timestamp) VALUES (?, ?, ?)"
      )
      stmt.setString(1, deviceName)
      stmt.setInt(2, value)
      // Parse ISO timestamp to OffsetDateTime
      val ts = OffsetDateTime.parse(timestamp)
      stmt.setObject(3, ts)
      stmt.executeUpdate()
      ()
    } finally {
      conn.close()
    }
  }
}