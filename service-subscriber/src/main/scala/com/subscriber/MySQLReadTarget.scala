package com.subscriber

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util.Properties

// The JDBC half of a reader for MySQL: one connection and its one prepared statement
// Owned by exactly one ReaderActor, which is why nothing here is synchronised
class MySQLReadTarget(dbUrl: String, dbUser: String, dbPass: String) extends ReadTarget {

  private val conn: Connection = {
    val props = new Properties()
    props.setProperty("user",     dbUser)
    props.setProperty("password", dbPass)
    // Planned once rather than on every execution; without it the group measures the query planner.
    // Matches MySQLBatchTarget and the Erlang arm, which prepares at init.
    props.setProperty("useServerPrepStmts", "true")
    props.setProperty("cachePrepStmts",     "true")
    DriverManager.getConnection(dbUrl, props)
  }

  // The MySQL spelling of the read group's query; see the read-group section of audit.md for why the
  // window is bounded. NOW(6), not NOW(), to match the microsecond resolution of the Postgres now().
  // Byte-identical to ?READ_SQL in db_read_backend_mysql.erl, or the group compares query plans
  // instead of runtimes. The rule is per backend: this pair must match, not the TimescaleDB pair.
  private val readStmt: PreparedStatement = conn.prepareStatement(
    "SELECT avg(Value), count(*) FROM Data WHERE Timestamp > NOW(6) - INTERVAL 5 SECOND")

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
