package com.subscriber

import java.sql.{Connection, PreparedStatement}

// The JDBC half of a batch writer for SQLite: one configured connection and its two statements.
// Owned by exactly one BatchWriterActor, or by one non-batch slot at a time, which is why nothing
// here is synchronised
class SQLiteBatchTarget extends BatchTarget {

  private val conn: Connection = SQLiteBackend.open()

  private val insertStmt: PreparedStatement = conn.prepareStatement(SQLiteBackend.InsertSql)
  private val statusStmt: PreparedStatement = conn.prepareStatement(SQLiteBackend.StatusSql)

  // One transaction per flush, so one fsync, with no fixed arity to build for. Not addBatch, which has
  // no esqlite analogue. Mirrors insert_batch/2 in db_backend_sqlite.erl, which explains IMMEDIATE.
  def writeBatch(rows: Seq[InsertRow]): Unit = SQLiteBackend.withWriteLock {
    exec("BEGIN IMMEDIATE")
    try {
      rows.foreach(bindAndRun)
      exec("COMMIT")
    } catch {
      // Rolled back on a failed COMMIT too, or every later BEGIN on this connection would fail.
      case e: Exception =>
        try exec("ROLLBACK") catch { case _: Exception => () }
        throw e
    }
  }

  // One row in autocommit, for the non-batch path
  def writeRow(row: InsertRow): Unit = SQLiteBackend.withWriteLock(bindAndRun(row))

  // Executed on this target's connection under the same lock, so a status write can queue ahead of a
  // data flush exactly as it does in the Erlang arm
  def writeStatus(deviceName: String, status: String): Unit = SQLiteBackend.withWriteLock {
    statusStmt.setString(1, deviceName)
    statusStmt.setString(2, status)
    statusStmt.executeUpdate()
  }

  // Timestamp is bound as the epoch-microsecond value itself; the column stores that unit.
  private def bindAndRun(r: InsertRow): Unit = {
    insertStmt.setString(1, r.deviceName)
    insertStmt.setInt(2, r.value)
    insertStmt.setLong(3, r.publisherEpochUs)
    insertStmt.executeUpdate()
  }

  private def exec(sql: String): Unit = {
    val stmt = conn.createStatement()
    try stmt.execute(sql) finally stmt.close()
  }

  def close(): Unit = {
    insertStmt.close()
    statusStmt.close()
    conn.close()
  }
}
