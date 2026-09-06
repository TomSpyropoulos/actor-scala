package com.subscriber

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util.Properties

// The JDBC half of a reader for TimescaleDB: one connection and its one prepared statement
// Owned by exactly one ReaderActor, which is why nothing here is synchronised
class TimescaleReadTarget(dbUrl: String, dbUser: String, dbPass: String) extends ReadTarget {

  private val conn: Connection = {
    val props = new Properties()
    props.setProperty("user",     dbUser)
    props.setProperty("password", dbPass)
    // Server-side prepare from the first execution instead of pgjdbc's default fifth, so the group
    // does not measure the query planner. Matches TimescaleBatchTarget and the Erlang arm.
    props.setProperty("prepareThreshold", "1")
    DriverManager.getConnection(dbUrl, props)
  }

  // The read group's query; see the read-group section of audit.md for why the window is bounded and
  // carries no device filter. Byte-identical to ?READ_SQL in db_read_backend_timescaledb.erl, or the
  // group compares query plans instead of runtimes. The rule is per backend: MySQLReadTarget and
  // db_read_backend_mysql.erl must match each other, not this pair.
  private val readStmt: PreparedStatement = conn.prepareStatement(
    "SELECT avg(Value), count(*) FROM Data WHERE Timestamp > now() - interval '5 seconds'")

  // Runs the read and drains its single aggregate row. The values are discarded, but the ResultSet is
  // still consumed and closed, so the measured time covers the whole round-trip.
  def read(): Unit = {
    val rs: ResultSet = readStmt.executeQuery()
    try while (rs.next()) ()
    finally rs.close()
  }

  def close(): Unit = {
    readStmt.close()
    conn.close()
  }
}
