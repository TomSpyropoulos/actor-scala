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
    // Server-side prepare from the first execution instead of pgjdbc's default fifth, so the read is
    // planned once rather than on every execution -- without it the group would be measuring the
    // query planner. Matches TimescaleBatchTarget, and the Erlang arm, which parses at init.
    props.setProperty("prepareThreshold", "1")
    DriverManager.getConnection(dbUrl, props)
  }

  // The query the whole read group is built on. Fixed text and no parameters: an aggregate over a
  // bounded recent window, so the rows it scans stay roughly constant as the table grows and read
  // latency does not drift upward with elapsed run time the way an unbounded scan would. No device
  // filter, so the reader needs no knowledge of which topics exist. Byte-identical to ?READ_SQL in
  // db_read_backend_timescaledb.erl -- if the two arms ever issue different SQL the group compares
  // query plans instead of runtimes, so keep them in sync.
  private val readStmt: PreparedStatement = conn.prepareStatement(
    "SELECT avg(Value), count(*) FROM Data WHERE Timestamp > now() - interval '5 seconds'")

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
