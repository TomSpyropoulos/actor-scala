package com.subscriber

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

// The JDBC half of a batch writer for MySQL: one connection, the full-size batch statement and the
// status statement. Owned by exactly one BatchWriterActor, which is why nothing here is synchronised
class MySQLBatchTarget(dbUrl: String, dbUser: String, dbPass: String, batchSize: Int)
    extends BatchTarget {

  private val conn: Connection = {
    val props = new Properties()
    props.setProperty("user",     dbUser)
    props.setProperty("password", dbPass)
    // Connector/J's equivalent of pgjdbc's prepareThreshold=1, so the batch statement is parsed once
    // like the Erlang arm's.
    props.setProperty("useServerPrepStmts", "true")
    props.setProperty("cachePrepStmts",     "true")
    DriverManager.getConnection(dbUrl, props)
  }

  // MySQL has no unnest, so a batch is a multi-row VALUES list, built full-size once here so the
  // common path is parsed once the way the TimescaleDB arrays are. Mirrors db_backend_mysql.erl.
  // Deliberately not addBatch/rewriteBatchedStatements: a JDBC-only trick with no mysql-otp
  // analogue would leave the two arms issuing structurally different writes.
  private def batchSql(rows: Int): String =
    "INSERT INTO Data (DeviceName, Value, Timestamp) VALUES " +
      Seq.fill(rows)("(?, ?, ?)").mkString(", ")

  private val fullBatchStmt: PreparedStatement = conn.prepareStatement(batchSql(batchSize))

  // Status rows share this writer's single connection rather than a pool of their own, so
  // DB_POOL_SIZE is the total connection count in both arms; see MySQLBackend.
  private val statusStmt: PreparedStatement = conn.prepareStatement(
    "INSERT INTO sensor_status (DeviceName, Status) VALUES (?, ?)")

  // Binds each row to its own three placeholders. A short buffer from a timeout flush gets a
  // statement built for its own length, since a VALUES list has fixed arity; that is the only path
  // that pays a parse.
  def writeBatch(rows: Seq[InsertRow]): Unit = {
    val stmt =
      if (rows.size == batchSize) fullBatchStmt
      else conn.prepareStatement(batchSql(rows.size))
    try {
      rows.zipWithIndex.foreach { case (r, i) =>
        val base = i * 3
        stmt.setString(base + 1, r.deviceName)
        stmt.setInt(base + 2, r.value)
        // Rebuilt from the epoch value the row carries; the raw payload string is not forwarded.
        stmt.setObject(base + 3, Clock.toLocalDateTimeUtc(r.publisherEpochUs))
      }
      stmt.executeUpdate()
    } finally {
      // Only the per-flush statement is closed; the full-size one is reused for the life of the
      // writer and closed in close().
      if (stmt ne fullBatchStmt) stmt.close()
    }
  }

  // Executed on the writer's connection, so a status write can queue ahead of a data flush exactly
  // as it does in the Erlang arm
  def writeStatus(deviceName: String, status: String): Unit = {
    statusStmt.setString(1, deviceName)
    statusStmt.setString(2, status)
    statusStmt.executeUpdate()
  }

  def close(): Unit = {
    fullBatchStmt.close()
    statusStmt.close()
    conn.close()
  }
}
