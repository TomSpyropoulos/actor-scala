package com.subscriber

import io.prometheus.client.Counter
import io.prometheus.client.Gauge
import io.prometheus.client.Summary
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

  val requestLatency: Summary = Summary.build()
    .name("subscriber_request_latency_milliseconds")
    .help("Latency of requests in milliseconds (Now - Payload Timestamp).")
    .quantile(0.5, 0.05)
    .quantile(0.95, 0.01)
    .quantile(0.99, 0.001)
    .quantile(0.999, 0.0001)
    .register()

  val e2eLatency: Summary = Summary.build()
    .name("subscriber_e2e_latency_milliseconds")
    .help("End-to-end latency in milliseconds (DB ack - Payload Timestamp).")
    .quantile(0.5, 0.05)
    .quantile(0.95, 0.01)
    .quantile(0.99, 0.001)
    .quantile(0.999, 0.0001)
    .register()

  val dbWriteLatency: Summary = Summary.build()
    .name("subscriber_db_write_latency_milliseconds")
    .help("Latency from subscriber receive to DB write ack, in milliseconds.")
    .quantile(0.5, 0.05)
    .quantile(0.95, 0.01)
    .quantile(0.99, 0.001)
    .quantile(0.999, 0.0001)
    .register()

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
