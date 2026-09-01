# Benchmark report

Source: `output/timescaledb` — 2 runs across 2 scenarios.

Latencies in ms, `msgs_s` (ingested) and `committed_s` (DB-committed) in msg/s, `cpu_cores` in cores, `mem_mb` in MB.

All figures cover **steady state only** — the first 30s of each run is excluded. Throughput and CPU therefore read higher than in pre-trim reports, which averaged the startup ramp in; that is a definition change, not an improvement.

Throughput/resource cells are mean ± stdev across reps. Latency cells are a **pooled quantile**: the reps' steady-state histogram buckets are summed and one quantile taken over the total, with the per-rep min–max in parentheses where the reps disagreed by more than 5%. A `mean` column accompanies each stage — it is exact, unquantized and never clamped, so it stays readable where a quantile lands in `+Inf`.

## Batching

| value | reps | msgs_s | committed_s | reads_s | cpu_cores | mem_mb | req_p50 | req_p95 | req_p99 | req_p999 | e2e_p50 | e2e_p95 | e2e_p99 | e2e_p999 | db_p50 | db_p95 | db_p99 | db_p999 | read_p50 | read_p95 | read_p99 | read_p999 | req_mean | e2e_mean | db_mean | read_mean |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| off | 1 | 19997.1 ± 0.0 | 3635.4 ± 0.0 | - | 5.0 ± 0.0 | 6594.0 ± 0.0 | 0.8 | 26.8 | 56.4 | 133.8 | 46763.2 | 60000.0 | 60000.0 | 60000.0 | 46762.7 | 60000.0 | 60000.0 | 60000.0 | - | - | - | - | 4.9 | 47171.4 | 47170.2 | - |

## Load

| value | reps | msgs_s | committed_s | reads_s | cpu_cores | mem_mb | req_p50 | req_p95 | req_p99 | req_p999 | e2e_p50 | e2e_p95 | e2e_p99 | e2e_p999 | db_p50 | db_p95 | db_p99 | db_p999 | read_p50 | read_p95 | read_p99 | read_p999 | req_mean | e2e_mean | db_mean | read_mean |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| p20 | 1 | 19999.2 ± 0.0 | 19990.9 ± 0.0 | 0.0 ± 0.0 | 4.1 ± 0.0 | 5581.9 ± 0.0 | 0.8 | 26.4 | 29.6 | 63.1 | 32.6 | 68.5 | 84.8 | 99.7 | 28.4 | 54.0 | 70.8 | 74.6 | - | - | - | - | 4.5 | 32.5 | 28.0 | - |

## Startup

`gate_startup_s` is the wait for the subscriber's metrics endpoint, `gate_ingest_s` the further wait until the first message was ingested — together, the cost of getting the runtime to the point where it is measurable at all. `ttss_s` is seconds from first message until throughput settles within 5% of its steady mean.

| scenario | gate_startup_s | gate_ingest_s | ttss_s | reps over warm-up | warmup_e2e_p99 |
|---|---|---|---|---|---|
| load_p20.env | 0.0 | 0.0 | 15.0 | 0 | 129.0 |
| timescale_batching_off.env | 0.0 | 0.0 | 15.0 | 0 | 42600.2 |

## Provenance

| key | value |
|---|---|
| warm-up trim | 30 s |
| run duration | 91 s |
| sample interval | 5 s |
| histogram buckets | 39 finite, fingerprint `af67de93` |
| reps included | 2 |
| reps contributing histogram buckets | 2 |
| reps excluded (counter reset) | 0 |
| reps not settled by trim boundary | 0 |
| latency estimator | pooled histogram quantiles |
