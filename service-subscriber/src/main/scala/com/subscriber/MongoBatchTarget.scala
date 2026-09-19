package com.subscriber

import scala.jdk.CollectionConverters._

// The driver half of a batch writer for MongoDB. Thin, like InfluxBatchTarget: the connection is
// borrowed from the shared write client per call, so there is nothing to prepare or own
class MongoBatchTarget extends BatchTarget {

  private val data   = MongoDBBackend.collection(MongoDBBackend.writeClient, "Data")
  private val status = MongoDBBackend.collection(MongoDBBackend.writeClient, "sensor_status")

  // One ordered insert command for the whole buffer, so a flush of any length waits for one journal
  // commit. Mirrors insert_batch/2 in db_backend_mongodb.erl.
  def writeBatch(rows: Seq[InsertRow]): Unit = {
    data.insertMany(
      rows.map(r => MongoDBBackend.dataDoc(r.deviceName, r.value, r.publisherEpochUs)).asJava)
    ()
  }

  // Sent on the shared client like data, so a status write can queue ahead of a flush exactly as it
  // does in the Erlang arm
  def writeStatus(deviceName: String, status: String): Unit = {
    this.status.insertOne(MongoDBBackend.statusDoc(deviceName, status))
    ()
  }

  // Nothing to release; the shared client belongs to MongoDBBackend.
  def close(): Unit = ()
}
