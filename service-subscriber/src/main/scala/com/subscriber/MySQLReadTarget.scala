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
    // Server-side prepared and cached, so the read is planned once rather than on every execution --
    // without it the group would be measuring the query planner. Matches MySQLBatchTarget, and the
    // Erlang arm, which prepares at init.
    props.setProperty("useServerPrepStmts", "true")
    props.setProperty("cachePrepStmts",     "true")
    DriverManager.getConnection(dbUrl, props)
  }

  // The MySQL spelling of the read the whole group is built on. Fixed text and no parameters: an
  // aggregate over a bounded recent window, so the rows it scans stay roughly constant as the table
  // grows and read latency does not drift upward with elapsed run time. NOW(6), not NOW(), to match
  // the microsecond resolution of the Postgres now() the TimescaleDB arm uses. Byte-identical to
  // ?READ_SQL in db_read_backend_mysql.erl -- the two arms must issue the same SQL against the same
  // backend, or the group compares query plans instead of runtimes. The TimescaleDB pair has its own
  // matching rule; the two backends are not expected to match each other.
  private val readStmt: PreparedStatement = conn.prepareStatement(
    "SELECT avg(Value), count(*) FROM Data WHERE Timestamp > NOW(6) - INTERVAL 5 SECOND")

  // Runs the read and drains its single aggregate row. The values are discarded -- this is load, not
  // a query whose answer anyone reads -- but the ResultSet is still consumed and closed, so the
  // measured time covers the whole round-trip and no cursor is left open on the connection.
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
