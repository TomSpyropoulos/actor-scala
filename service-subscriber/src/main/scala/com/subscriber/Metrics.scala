package com.subscriber

import io.prometheus.client.Counter
import io.prometheus.client.Gauge
import io.prometheus.client.Histogram
import io.prometheus.client.exporter.HTTPServer
import io.prometheus.client.hotspot.DefaultExports

// Declares all Prometheus metrics and manages the HTTP scrape server for the subscriber service
object Metrics {
  val requestCount: Counter = Counter.build()
    .name("subscriber_requests_total")
    .help("Total requests processed by the subscriber.")
    .register()

  // Rows actually committed to the DB, incremented on the write-ack path (separate from ingest count)
  val committedCount: Counter = Counter.build()
    .name("subscriber_committed_total")
    .help("Total rows committed to the database.")
    .register()

  // Latency histogram bounds in milliseconds, shared verbatim with the Erlang arm's
  // service_subscriber_metrics.erl. Changing them in one repo silently destroys cross-arm latency
  // comparability; changing them at all invalidates comparison against previously collected sweeps.
  private val LatencyBuckets: Array[Double] = Array(
    0.05, 0.075, 0.1, 0.15, 0.2, 0.3, 0.4,
    0.5, 0.75, 1.0, 1.5, 2.0, 3.0, 4.0, 5.0, 7.5, 10.0, 15.0, 20.0, 30.0, 40.0, 50.0, 75.0,
    100.0, 150.0, 200.0, 300.0, 400.0, 500.0, 750.0, 1000.0, 1500.0, 2000.0, 3000.0, 5000.0,
    7500.0, 10000.0, 20000.0, 60000.0
  )

  val requestLatency: Histogram = Histogram.build()
    .name("subscriber_request_latency_milliseconds")
    .help("Latency of requests in milliseconds (Now - Payload Timestamp).")
    .buckets(LatencyBuckets*)
    .register()

  val e2eLatency: Histogram = Histogram.build()
    .name("subscriber_e2e_latency_milliseconds")
    .help("End-to-end latency in milliseconds (DB ack - Payload Timestamp).")
    .buckets(LatencyBuckets*)
    .register()

  val dbWriteLatency: Histogram = Histogram.build()
    .name("subscriber_db_write_latency_milliseconds")
    .help("Latency from subscriber receive to DB write ack, in milliseconds.")
    .buckets(LatencyBuckets*)
    .register()

  // Records one committed row: stamps the DB ack, counts it, observes both latencies. Backends used
  // to hand-roll this and each stamped its own ack time, so one that drifted produced a run that
  // looked healthy and was silently non-comparable. This is the only definition.
  def recordCommit(publisherEpochUs: Long, subscriberReceiveUs: Long): Unit = {
    val ackUs = Clock.nowMicros()
    committedCount.inc()
    observeAck(ackUs, publisherEpochUs, subscriberReceiveUs)
  }

  // Batch variant: reads the ack clock once per flush rather than once per row, so every row in a
  // batch shares one timestamp — matching how the Erlang arm stamps a batch ack.
  def recordCommitBatch[A](rows: Seq[A])(publisherEpochUs: A => Long,
                                         subscriberReceiveUs: A => Long): Unit = {
    val ackUs = Clock.nowMicros()
    committedCount.inc(rows.size.toDouble)
    rows.foreach(r => observeAck(ackUs, publisherEpochUs(r), subscriberReceiveUs(r)))
  }

  // Clamped at 0 for clock skew, mirroring the Erlang arm's max(0, ...) on the same two latencies.
  private def observeAck(ackUs: Long, publisherEpochUs: Long, subscriberReceiveUs: Long): Unit = {
    e2eLatency.observe(math.max(0L, ackUs - publisherEpochUs) / 1000.0)
    dbWriteLatency.observe(math.max(0L, ackUs - subscriberReceiveUs) / 1000.0)
  }

  // Sensor liveness gauge: 1 = ALIVE, 0 = MISSING; one label series per device
  val sensorUp: Gauge = Gauge.build()
    .name("subscriber_sensor_up")
    .help("Sensor liveness: 1 = ALIVE, 0 = MISSING.")
    .labelNames("device")
    .register()

  private var server: Option[HTTPServer] = None

  // Starts the Prometheus HTTP server and registers JVM default exports; idempotent
  def init(port: Int = 8081): Unit = {
    if (server.isEmpty) {
      DefaultExports.initialize()
      server = Some(new HTTPServer(port))
      println(s"Prometheus metrics server started on port $port")
    }
  }

  // Shuts down the scrape server; called from the JVM shutdown hook before the actor system terminates
  def stop(): Unit = {
    server.foreach(_.stop())
    server = None
  }
}
