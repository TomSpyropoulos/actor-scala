package com.subscriber

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

// The JDBC half of a batch writer for TimescaleDB: one connection and its two prepared statements
// Owned by exactly one BatchWriterActor, which is why nothing here is synchronised
class TimescaleBatchTarget(dbUrl: String, dbUser: String, dbPass: String) extends BatchTarget {

  private val conn: Connection = {
    val props = new Properties()
    props.setProperty("user",     dbUser)
    props.setProperty("password", dbPass)
    // Server-side prepare from the first execution instead of pgjdbc's default fifth, so the batch
    // statement is parsed once like the Erlang arm's, which epgsql parses at init.
    props.setProperty("prepareThreshold", "1")
    DriverManager.getConnection(dbUrl, props)
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

  // Binds the flushed buffer as three arrays and executes the pre-prepared statement
  def writeBatch(rows: Seq[InsertRow]): Unit = {
    val names  = rows.map(_.deviceName).toArray
    val values = rows.map(r => Integer.valueOf(r.value)).toArray
    // The reading's own instant, rebuilt from the epoch value the row carries -- the raw payload
    // string is not forwarded, so this is the only conversion on the write path.
    val stamps = rows.map(r => Clock.toOffsetDateTime(r.publisherEpochUs)).toArray

    batchStmt.setArray(1, conn.createArrayOf("text",        names.asInstanceOf[Array[Object]]))
    batchStmt.setArray(2, conn.createArrayOf("int4",        values.asInstanceOf[Array[Object]]))
    batchStmt.setArray(3, conn.createArrayOf("timestamptz", stamps.asInstanceOf[Array[Object]]))
    batchStmt.executeUpdate()
  }

  // Executed on the writer's connection, so a status write can queue ahead of a data flush exactly
  // as it does in the Erlang arm
  def writeStatus(deviceName: String, status: String): Unit = {
    statusStmt.setString(1, deviceName)
    statusStmt.setString(2, status)
    statusStmt.executeUpdate()
  }

  def close(): Unit = {
    batchStmt.close()
    statusStmt.close()
    conn.close()
  }
}
