package com.subscriber

import scala.concurrent.{ExecutionContext, Future}

/** Trait defining the write interface for pluggable database backends.
  *
  * All implementations must be thread-safe: the same instance is shared
  * across all TopicActor instances, which run concurrently on separate
  * dispatcher threads.
  *
  * The active backend is selected at JVM startup by [[Database]] based on
  * the DB_BACKEND environment variable. To add a new backend: implement
  * this trait and add a case to [[Database]].
  */
trait DatabaseBackend {

  /** Insert one sensor reading into the Data table.
    *
    * Runs asynchronously on the provided ExecutionContext. The returned
    * Future completes when the row has been durably written; callers use
    * the completion time to measure subscriber→DB and end-to-end latency.
    */
  def insertData(deviceName: String, value: Int, timestamp: String)(
      implicit ec: ExecutionContext): Future[Unit]

  /** Insert or update the liveness status of a sensor device. */
  def insertStatus(deviceName: String, status: String)(
      implicit ec: ExecutionContext): Future[Unit]

  /** Release connection pool or other resources. Called once at JVM shutdown
    * before the actor system terminates.
    */
  def close(): Unit
}
