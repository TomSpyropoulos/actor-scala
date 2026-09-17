package com.subscriber

import java.sql.{Connection, PreparedStatement, ResultSet}

// The JDBC half of a reader for SQLite: one configured connection and its one prepared statement
// Owned by exactly one ReaderActor, which is why nothing here is synchronised
class SQLiteReadTarget extends ReadTarget {

  private val conn: Connection = SQLiteBackend.open()

  // Byte-identical to ?READ_SQL in db_read_backend_sqlite.erl, or the group compares query plans
  // instead of runtimes. The rule is per backend: this pair must match, not the other databases'.
  private val readStmt: PreparedStatement = conn.prepareStatement(
    "SELECT avg(Value), count(*) FROM Data " +
    "WHERE Timestamp > CAST(unixepoch('subsec') * 1000000 AS INTEGER) - 5000000")

  // Runs the read and drains its single aggregate row. The values are discarded, but the ResultSet is
  // still consumed and closed, so the measured time covers the whole query.
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
