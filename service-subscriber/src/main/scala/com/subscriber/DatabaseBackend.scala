package com.subscriber

import scala.concurrent.{ExecutionContext, Future}

// Shared write interface for pluggable DB backends. The same instance is used by all TopicActors concurrently
trait DatabaseBackend {

  // publisherEpochUs is both the Timestamp column value (via Clock.toOffsetDateTime) and the e2e
  // start, so the reading's instant crosses here once. SubscriberReceiveUs only times the db write.
  def insertData(deviceName: String, value: Int,
                 publisherEpochUs: Long, subscriberReceiveUs: Long)(
      implicit ec: ExecutionContext): Future[Unit]

  // Persists the liveness status transition for a sensor device
  def insertStatus(deviceName: String, status: String)(
      implicit ec: ExecutionContext): Future[Unit]

  // Releases connection pool and any other resources. Called once at JVM shutdown before the actor system terminates
  def close(): Unit
}
