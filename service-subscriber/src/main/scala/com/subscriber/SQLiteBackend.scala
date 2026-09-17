package com.subscriber

import org.apache.pekko.actor.ActorSystem

import java.nio.file.{Files, Paths}
import java.sql.{Connection, DriverManager}
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.locks.ReentrantLock

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

// The connection half this backend shares with its batch and read targets: where the file is and how
// a connection is set up. Not built from DbConfig, whose five keys describe a server; SQLite needs a
// path and the schema directory instead
object SQLiteBackend {
  val path: String    = sys.env.getOrElse("DB_PATH",     "/var/lib/sqlite/epu.db")
  val initDir: String = sys.env.getOrElse("DB_INIT_DIR", "/sqlite/init")

  val InsertSql = "INSERT INTO Data (DeviceName, Value, Timestamp) VALUES (?, ?, ?)"
  val StatusSql = "INSERT INTO sensor_status (DeviceName, Status) VALUES (?, ?)"

  // connection.sql first, so busy_timeout is in force before init.sql contends for the schema lock.
  private val SetupFiles = Seq("connection.sql", "init.sql")

  // Fair, so writers queue in arrival order as db_backend_sqlite_lock.erl serves them. Every write
  // and every connection setup holds it; see finding R in audit.md.
  private val WriteLock = new ReentrantLock(true)

  // Runs write while holding the lock, releasing it even if write throws
  def withWriteLock[T](write: => T): T = {
    WriteLock.lock()
    try write finally WriteLock.unlock()
  }

  // Opens a connection and applies both setup files under the write lock, closing it again if either
  // fails, so a bad path or schema fails the subscriber at startup. Mirrors open/0 in
  // db_backend_sqlite.erl.
  def open(): Connection = {
    val conn = DriverManager.getConnection(s"jdbc:sqlite:$path")
    try {
      val stmt = conn.createStatement()
      try withWriteLock(SetupFiles.foreach(f => statements(f).foreach(stmt.execute)))
      finally stmt.close()
      conn
    } catch {
      case e: Throwable => conn.close(); throw e
    }
  }

  // Whole-line `--` comments dropped, the rest split on semicolons, as both files' headers require.
  // Must stay equivalent to statements/1 in db_backend_sqlite.erl.
  private def statements(file: String): Seq[String] =
    Files.readAllLines(Paths.get(initDir, file)).asScala
      .filterNot(_.trim.startsWith("--"))
      .mkString("\n")
      .split(";").map(_.trim).filter(_.nonEmpty).toSeq
}

// Concrete DatabaseBackend for SQLite: non-batching borrows a writer slot per row; batching delegates
// to the shared BatchWriterPool, supplying an SQLiteBatchTarget
class SQLiteBackend(implicit system: ActorSystem) extends DatabaseBackend {

  // --- Non-batching: DB_POOL_SIZE slots, each a connection with its statements prepared ---
  // Not HikariCP: sqlite-jdbc caches no prepared statements, so a pooled connection would parse the
  // INSERT on every row where the Erlang worker prepares once. Blocking in take() on
  // blocking-io-dispatcher is the same cap the other backends get from getConnection.
  private val slots: Option[LinkedBlockingQueue[SQLiteBatchTarget]] =
    if (!BatchConfig.enabled) {
      val q = new LinkedBlockingQueue[SQLiteBatchTarget]()
      (1 to BatchConfig.writers).foreach(_ => q.put(new SQLiteBatchTarget()))
      Some(q)
    } else None

  // --- Batching: shared writer pool, backed by this backend's JDBC target ---
  private val writers: Option[BatchWriterPool] =
    if (BatchConfig.enabled)
      Some(new BatchWriterPool(BatchConfig.size, BatchConfig.timeoutMs, BatchConfig.writers,
                               () => new SQLiteBatchTarget()))
    else None

  // Runs f on a borrowed slot and always hands it back, so a failed write cannot shrink the pool
  private def withSlot[T](f: SQLiteBatchTarget => T): T = {
    val q    = slots.get
    val slot = q.take()
    try f(slot) finally q.put(slot)
  }

  // Routes to the writer pool (batch) or writes inline on a slot (non-batch); records latency after the commit
  def insertData(deviceName: String, value: Int,
                 publisherEpochUs: Long, subscriberReceiveUs: Long)(
      implicit ec: ExecutionContext): Future[Unit] = {
    writers match {
      case Some(pool) =>
        pool.route(InsertRow(deviceName, value, publisherEpochUs, subscriberReceiveUs))
        Future.successful(())
      case None =>
        Future {
          withSlot(_.writeRow(InsertRow(deviceName, value, publisherEpochUs, subscriberReceiveUs)))
          // Metrics owns what a commit means, but the call site is per-backend on this side, so a
          // backend that skipped this would look healthy and be silently non-comparable.
          Metrics.recordCommit(publisherEpochUs, subscriberReceiveUs)
        }
    }
  }

  // Written on the same slots as data, so DB_POOL_SIZE is the total connection count here as it is in
  // Erlang, where insert_status shares the worker pool.
  def insertStatus(deviceName: String, status: String)(
      implicit ec: ExecutionContext): Future[Unit] = {
    writers match {
      case Some(pool) =>
        pool.route(InsertStatus(deviceName, status))
        Future.successful(())
      case None =>
        Future(withSlot(_.writeStatus(deviceName, status)))
    }
  }

  // Stops all writers, letting them drain, and closes every idle slot
  def close(): Unit = {
    writers.foreach(_.close())
    slots.foreach(q => q.asScala.foreach(_.close()))
  }
}
