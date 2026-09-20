package com.subscriber

import com.mongodb.{MongoClientSettings, MongoCredential, ServerAddress, WriteConcern}
import com.mongodb.client.{MongoClient, MongoClients, MongoCollection}
import com.mongodb.connection.ServerMonitoringMode

import org.apache.pekko.actor.ActorSystem
import org.bson.Document

import java.util.Date

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

// The driver half this backend shares with its batch and read targets: the two clients, the write
// concern and the document builders. Not built from DbConfig.urlFor, which assembles a JDBC URL. The
// same five keys are read through DbConfig
object MongoDBBackend {
  // Journaled, so an ack means the same fsync the SQL commits mean.
  // Mirrors ?WRITE_CONCERN in db_backend_mongodb.erl.
  val Journaled: WriteConcern = WriteConcern.W1.withJournal(true)

  // One client per role for the process, not one per writer: each adds a monitoring socket and
  // threads of its own. The pool opens all its connections up front, so DB_POOL_SIZE is what the
  // server sees, plus the one POLL monitor socket.
  private def client(poolSize: Int): MongoClient =
    MongoClients.create(MongoClientSettings.builder()
      .applyToClusterSettings(b => { b.hosts(List(new ServerAddress(DbConfig.host, DbConfig.port.toInt)).asJava); () })
      .applyToConnectionPoolSettings(b => { b.maxSize(poolSize).minSize(poolSize); () })
      .applyToServerSettings(b => { b.serverMonitoringMode(ServerMonitoringMode.POLL); () })
      .credential(MongoCredential.createCredential(DbConfig.user, "admin", DbConfig.password.toCharArray))
      .writeConcern(Journaled)
      .build())

  // Lazy, so a run with reads off never opens the read client at all. Neither is closed: writers
  // drain after close() returns, the same reason InfluxDBBackend's HttpClient outlives the backend.
  lazy val writeClient: MongoClient = client(BatchConfig.writers)
  lazy val readClient: MongoClient  = client(ReadConfig.readers)

  // DB_NAME resolved in one place, so the writers and readers cannot end up on different databases
  def collection(c: MongoClient, name: String): MongoCollection[Document] =
    c.getDatabase(DbConfig.name).getCollection(name)

  // One reading as a document. Value stays an Int so it is stored as int32. Nothing in the schema
  // would catch a double. Mirrors data_doc/3 in db_backend_mongodb.erl.
  def dataDoc(deviceName: String, value: Int, publisherEpochUs: Long): Document =
    new Document("Timestamp", Clock.toDateMillis(publisherEpochUs))
      .append("DeviceName", deviceName)
      .append("Value", Int.box(value))

  // reportedat is the client's clock, since MongoDB has no insert-time default.
  def statusDoc(deviceName: String, status: String): Document =
    new Document("DeviceName", deviceName).append("Status", status).append("reportedat", new Date())
}

// Concrete DatabaseBackend for MongoDB: non-batching inserts one document per reading on the shared
// write client. Batching delegates to the shared BatchWriterPool, supplying a MongoBatchTarget
class MongoDBBackend(implicit system: ActorSystem) extends DatabaseBackend {

  private val data       = MongoDBBackend.collection(MongoDBBackend.writeClient, "Data")
  private val statusColl = MongoDBBackend.collection(MongoDBBackend.writeClient, "sensor_status")

  // The client connects lazily, so ping now: a database that is not up fails the subscriber at
  // startup the way a JDBC connect does, instead of ingesting and committing nothing.
  MongoDBBackend.writeClient.getDatabase(DbConfig.name).runCommand(new Document("ping", 1))

  // --- Batching: shared writer pool, backed by this backend's driver target ---
  private val writers: Option[BatchWriterPool] =
    if (BatchConfig.enabled)
      Some(new BatchWriterPool(BatchConfig.size, BatchConfig.timeoutMs, BatchConfig.writers,
                               () => new MongoBatchTarget()))
    else None

  // Routes to the writer pool (batch) or inserts inline (non-batch). Records latency after the DB ack.
  // Blocking in the pool's checkout on blocking-io-dispatcher is the cap, as HikariCP's is.
  def insertData(deviceName: String, value: Int,
                 publisherEpochUs: Long, subscriberReceiveUs: Long)(
      implicit ec: ExecutionContext): Future[Unit] = {
    writers match {
      case Some(pool) =>
        pool.route(InsertRow(deviceName, value, publisherEpochUs, subscriberReceiveUs))
        Future.successful(())
      case None =>
        Future {
          data.insertOne(MongoDBBackend.dataDoc(deviceName, value, publisherEpochUs))
          // Metrics owns what a commit means, but the call site is per-backend on this side, so a
          // backend that skipped this would look healthy and be silently non-comparable.
          Metrics.recordCommit(publisherEpochUs, subscriberReceiveUs)
        }
    }
  }

  // Sent over the same pool as data, so DB_POOL_SIZE bounds the total here as it already does in
  // Erlang, where insert_status shares the worker pool.
  def insertStatus(deviceName: String, status: String)(
      implicit ec: ExecutionContext): Future[Unit] = {
    writers match {
      case Some(pool) =>
        pool.route(InsertStatus(deviceName, status))
        Future.successful(())
      case None =>
        Future {
          statusColl.insertOne(MongoDBBackend.statusDoc(deviceName, status))
          ()
        }
    }
  }

  // Stops all writers, letting them drain. The shared client outlives this by design. See above.
  def close(): Unit = writers.foreach(_.close())
}
