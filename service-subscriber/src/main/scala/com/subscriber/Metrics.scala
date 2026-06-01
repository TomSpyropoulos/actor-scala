package com.subscriber

import io.prometheus.client.Counter
import io.prometheus.client.Gauge
import io.prometheus.client.Summary
import io.prometheus.client.exporter.HTTPServer
import io.prometheus.client.hotspot.DefaultExports

/**
 * Manages Prometheus metrics and the HTTP server for scraping.
 * This object centralizes all monitoring logic for the Service Subscriber.
 */
object Metrics {
  /**
   * Request count metric: A counter that increments for each processed MQTT message.
   */
  val requestCount: Counter = Counter.build()
    .name("subscriber_requests_total")
    .help("Total requests processed by the subscriber.")
    .register()

  /**
   * Latency metric: A summary that records the time difference between the
   * payload's timestamp (source) and the current time (processing).
   * Provides quantiles (p50, p95, p99, p999).
   */
  val requestLatency: Summary = Summary.build()
    .name("subscriber_request_latency_milliseconds")
    .help("Latency of requests in milliseconds (Now - Payload Timestamp).")
    .quantile(0.5, 0.05)    // Median latency
    .quantile(0.95, 0.01)   // 95th percentile
    .quantile(0.99, 0.001)  // 99th percentile
    .quantile(0.999, 0.0001)// 99.9th percentile
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

  /** Sensor liveness gauge: 1 = ALIVE, 0 = MISSING. One label series per device. */
  val sensorUp: Gauge = Gauge.build()
    .name("subscriber_sensor_up")
    .help("Sensor liveness: 1 = ALIVE, 0 = MISSING.")
    .labelNames("device")
    .register()

  private var server: Option[HTTPServer] = None

  /**
   * Initializes metrics and starts the HTTP server.
   * Also initializes standard Hotspot JVM metrics (GC, memory, threads).
   * @param port The port to listen on for scraping (default: 8081).
   */
  def init(port: Int = 8081): Unit = {
    if (server.isEmpty) {
      // Initialize JVM-wide metrics for visibility into the runtime
      DefaultExports.initialize()
      // Start the Prometheus scraper endpoint
      server = Some(new HTTPServer(port))
      println(s"Prometheus metrics server started on port $port")
    }
  }

  /**
   * Stops the HTTP server gracefully during application shutdown.
   */
  def stop(): Unit = {
    server.foreach(_.stop())
    server = None
  }
}
