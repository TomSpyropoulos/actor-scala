package com.subscriber

import org.apache.pekko.actor.ActorSystem

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration
import java.util.concurrent.Semaphore

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}

// The HTTP half this backend shares with its batch and read targets: URLs, the client, the
// line-protocol builders and the readiness probe. Not built from DbConfig.urlFor, which assembles a
// JDBC URL; the same five keys are read through DbConfig, plus the two InfluxDB needs
object InfluxDBBackend {
  val org: String    = sys.env.getOrElse("DB_ORG",   "epu")
  val token: String  = sys.env.getOrElse("DB_TOKEN", "epu-benchmark-token")
  val bucket: String = DbConfig.name

  private val baseUrl: String = s"http://${DbConfig.host}:${DbConfig.port}"

  // precision=us is load-bearing: without it InfluxDB reads the number as nanoseconds, every point
  // lands in 1970 and the read window is silently empty. See finding L.
  val WriteUrl: String = s"$baseUrl/api/v2/write?org=$org&bucket=$bucket&precision=us"
  val QueryUrl: String = s"$baseUrl/api/v2/query?org=$org"

  private val RequestTimeout = Duration.ofSeconds(10)
  private val ReadyAttempts  = 60
  private val ReadyRetryMs   = 1000L

  // One client for the process, not one per writer: each carries a selector thread and its own
  // executor, so fifty at pool_50 would be thread and memory cost with nothing to do with the
  // database. HTTP_1_1 is pinned because the default attempts an h2c upgrade, and multiplexing
  // would hand this arm wire concurrency inets cannot have. See finding J.
  // Nothing closes it: HttpClient became AutoCloseable in JDK 21 and this image runs 17.
  val Http: HttpClient = HttpClient.newBuilder()
    .version(HttpClient.Version.HTTP_1_1)
    .connectTimeout(Duration.ofSeconds(5))
    .build()

  private def request(url: String, contentType: String): HttpRequest.Builder =
    HttpRequest.newBuilder(URI.create(url))
      .header("Authorization", s"Token $token")
      .header("Content-Type", contentType)
      .timeout(RequestTimeout)

  // Returns the body only on the expected status, so a caller can treat a normal return as the DB
  // ack. Throwing is what marks a batch uncommitted in BatchWriterActor.
  private def send(req: HttpRequest, expect: Int): String = {
    val res = Http.send(req, HttpResponse.BodyHandlers.ofString())
    if (res.statusCode() != expect)
      throw new RuntimeException(s"InfluxDB returned ${res.statusCode()}: ${res.body()}")
    res.body()
  }

  // Posts line protocol and returns once InfluxDB has acked it with 204
  def write(body: String): Unit =
    send(request(WriteUrl, "text/plain; charset=utf-8")
           .POST(HttpRequest.BodyPublishers.ofString(body)).build(), 204)

  // Runs one Flux script and returns the whole annotated-CSV body, so the measured time covers the
  // full round-trip the way MySQLReadTarget's explicit result-set drain does
  def query(flux: String): String =
    send(request(QueryUrl, "application/vnd.flux")
           .POST(HttpRequest.BodyPublishers.ofString(flux)).build(), 200)

  // One reading as a line-protocol point. The trailing i keeps Value an integer: field type is
  // fixed by the first write into a shard, so dropping it would silently store floats. Nothing is
  // escaped; finding N records why that is safe here. Mirrors line/3 in db_backend_influxdb.erl.
  def dataLine(deviceName: String, value: Int, publisherEpochUs: Long): String =
    s"Data,DeviceName=$deviceName Value=${value}i $publisherEpochUs"

  // One status transition, with no timestamp so the server assigns one -- the equivalent of the
  // DEFAULT NOW() both SQL schemas give reportedat. The CHECK constraint has no InfluxDB analogue.
  def statusLine(deviceName: String, status: String): String =
    s"""sensor_status,DeviceName=$deviceName Status="$status""""

  // Blocks until the bucket answers, so a database that is not up fails the subscriber at startup
  // the way a JDBC connect does. HTTP connects lazily, so without this the subscriber would start,
  // ingest happily and commit nothing -- a rep bench.sh would score as valid. See finding I.
  def awaitReady(): Unit = {
    val req = HttpRequest.newBuilder(URI.create(s"$baseUrl/api/v2/buckets?name=$bucket"))
      .header("Authorization", s"Token $token").timeout(RequestTimeout).GET().build()
    probe(req, ReadyAttempts)
  }

  @tailrec
  private def probe(req: HttpRequest, attempts: Int): Unit = {
    if (attempts <= 0) throw new IllegalStateException(s"InfluxDB not ready at $baseUrl")
    val ok =
      try Http.send(req, HttpResponse.BodyHandlers.ofString()).statusCode() == 200
      catch {
        case e: InterruptedException => Thread.currentThread().interrupt(); throw e
        case _: Exception            => false
      }
    if (!ok) { Thread.sleep(ReadyRetryMs); probe(req, attempts - 1) }
  }
}

// Concrete DatabaseBackend for InfluxDB: non-batching posts one point per reading under a
// concurrency cap; batching delegates to the shared BatchWriterPool, supplying an InfluxBatchTarget
class InfluxDBBackend(implicit system: ActorSystem) extends DatabaseBackend {

  InfluxDBBackend.awaitReady()

  // The other two backends cap concurrency by blocking in HikariDataSource.getConnection on
  // blocking-io-dispatcher. This is the same cap on the same threads, so application.conf's
  // fixed-pool-size coupling still holds; a private executor would make pool_* measure a different
  // kind of overload here than for the other two. See finding J.
  private val inFlight = new Semaphore(BatchConfig.writers)

  // --- Batching: shared writer pool, backed by this backend's HTTP target ---
  private val writers: Option[BatchWriterPool] =
    if (BatchConfig.enabled)
      Some(new BatchWriterPool(BatchConfig.size, BatchConfig.timeoutMs, BatchConfig.writers,
                               () => new InfluxBatchTarget()))
    else None

  // Routes to the writer pool (batch) or posts inline under the cap (non-batch); records latency after the DB ack
  def insertData(deviceName: String, value: Int,
                 publisherEpochUs: Long, subscriberReceiveUs: Long)(
      implicit ec: ExecutionContext): Future[Unit] = {
    writers match {
      case Some(pool) =>
        pool.route(InsertRow(deviceName, value, publisherEpochUs, subscriberReceiveUs))
        Future.successful(())
      case None =>
        Future {
          inFlight.acquire()
          try {
            InfluxDBBackend.write(InfluxDBBackend.dataLine(deviceName, value, publisherEpochUs))
            // Metrics owns what a commit means, but the call site is per-backend on this side, so a
            // backend that skipped this would look healthy and be silently non-comparable.
            Metrics.recordCommit(publisherEpochUs, subscriberReceiveUs)
          } finally inFlight.release()
        }
    }
  }

  // Sent over the same client and under the same cap as data, so DB_POOL_SIZE bounds the total here
  // as it already does in Erlang, where insert_status shares the worker pool.
  def insertStatus(deviceName: String, status: String)(
      implicit ec: ExecutionContext): Future[Unit] = {
    writers match {
      case Some(pool) =>
        pool.route(InsertStatus(deviceName, status))
        Future.successful(())
      case None =>
        Future {
          inFlight.acquire()
          try InfluxDBBackend.write(InfluxDBBackend.statusLine(deviceName, status))
          finally inFlight.release()
        }
    }
  }

  // Stops all writers, letting them drain. The shared HttpClient outlives this by design; see above.
  def close(): Unit = writers.foreach(_.close())
}
