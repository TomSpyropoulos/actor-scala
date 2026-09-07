package com.subscriber

// The HTTP half of a batch writer for InfluxDB. Thin next to the JDBC targets: there is no
// connection and no statement to prepare, so a batch is only its rows joined by newlines
class InfluxBatchTarget extends BatchTarget {

  // Line protocol has no fixed arity, so unlike MySQLBatchTarget a short flush from a timeout needs
  // no statement of its own and pays no parse. Mirrors insert_batch/2 in db_backend_influxdb.erl.
  def writeBatch(rows: Seq[InsertRow]): Unit =
    InfluxDBBackend.write(
      rows.map(r => InfluxDBBackend.dataLine(r.deviceName, r.value, r.publisherEpochUs))
        .mkString("\n"))

  // Sent on the shared client like data, so a status write can queue ahead of a flush exactly as it
  // does in the Erlang arm
  def writeStatus(deviceName: String, status: String): Unit =
    InfluxDBBackend.write(InfluxDBBackend.statusLine(deviceName, status))

  // Nothing to release: every writer shares one HttpClient, which JDK 17 cannot close anyway. The
  // JDBC targets close a real connection here.
  def close(): Unit = ()
}
