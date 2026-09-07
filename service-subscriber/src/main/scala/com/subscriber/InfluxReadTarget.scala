package com.subscriber

// The HTTP half of a reader: one Flux script, built once with the bucket interpolated. It owns no
// connection, unlike the JDBC read targets; every reader shares the process-wide HttpClient
class InfluxReadTarget extends ReadTarget {

  // The InfluxDB spelling of the read group's query; see the read-group section of audit.md for why
  // the window is bounded. group() and reduce are load-bearing for equivalence with the SQL
  // backends' avg/count, and finding L records why.
  //
  // Byte-identical to ?READ_FLUX in db_read_backend_influxdb.erl, or the group compares query plans
  // instead of runtimes. The rule is per backend: this pair must match each other, not a SQL pair.
  private val flux: String = Seq(
    s"""from(bucket: "${InfluxDBBackend.bucket}")""",
    """  |> range(start: -5s)""",
    """  |> filter(fn: (r) => r._measurement == "Data" and r._field == "Value")""",
    """  |> group()""",
    """  |> reduce(identity: {count: 0, sum: 0.0},""",
    """            fn: (r, accumulator) => ({count: accumulator.count + 1,""",
    """                                      sum: accumulator.sum + float(v: r._value)}))""",
    """  |> map(fn: (r) => ({mean: r.sum / float(v: r.count), count: r.count}))"""
  ).mkString("\n")

  // Runs the read to completion and discards the body. Throwing marks the read failed, so the
  // reader leaves it uncounted rather than inflating the read rate with queries that never answered.
  def read(): Unit = {
    InfluxDBBackend.query(flux)
    ()
  }

  // Nothing to release; see InfluxBatchTarget.
  def close(): Unit = ()
}
