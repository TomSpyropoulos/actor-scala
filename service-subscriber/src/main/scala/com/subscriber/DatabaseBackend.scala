package com.subscriber

import scala.concurrent.{ExecutionContext, Future}

// Shared write interface for pluggable DB backends; the same instance is used by all TopicActors concurrently
trait DatabaseBackend {

  // publisherEpochMs and subscriberReceiveMs are forwarded so the backend can record e2e and db_write latency at flush time
  def insertData(deviceName: String, value: Int, timestamp: String,
                 publisherEpochMs: Long, subscriberReceiveMs: Long)(
      implicit ec: ExecutionContext): Future[Unit]

  // Persists the liveness status transition for a sensor device
  def insertStatus(deviceName: String, status: String)(
      implicit ec: ExecutionContext): Future[Unit]

  // Releases connection pool and any other resources; called once at JVM shutdown before the actor system terminates
  def close(): Unit
}
