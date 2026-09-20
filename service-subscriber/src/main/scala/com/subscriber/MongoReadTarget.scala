package com.subscriber

import org.bson.{BsonArray, Document}
import org.bson.conversions.Bson

import scala.jdk.CollectionConverters._

// The driver half of a reader for MongoDB: the parsed pipeline, run on the shared read client, whose
// pool holds one connection per reader
class MongoReadTarget extends ReadTarget {

  private val data = MongoDBBackend.collection(MongoDBBackend.readClient, "Data")

  // Byte-identical to ?READ_PIPELINE in db_read_backend_mongodb.erl, which says why it is JSON text.
  // The rule is per backend: this pair must match each other, not a SQL pair.
  private val pipeline: java.util.List[Bson] =
    BsonArray.parse(
      """[{"$match":{"$expr":{"$gt":["$Timestamp",{"$subtract":["$$NOW",5000]}]}}},""" +
      """{"$group":{"_id":null,"avg":{"$avg":"$Value"},"count":{"$sum":1}}}]""")
      .getValues.asScala.map(v => v.asDocument(): Bson).asJava

  // Runs the aggregate and drains its cursor. Throwing marks the read failed, so the reader leaves
  // it uncounted.
  def read(): Unit = {
    data.aggregate(pipeline).into(new java.util.ArrayList[Document]())
    ()
  }

  // Nothing to release. See MongoBatchTarget.
  def close(): Unit = ()
}
