# IoT Data Pipeline (Scala & Pekko)

A high-performance, containerized IoT data pipeline implemented using **Scala**, **Pekko Actors**, and **Pekko Streams**. This project demonstrates how to build a scalable messaging system that handles real-time sensor data with backpressure and efficient resource management.

## 🏗️ System Architecture

The system consists of the following components:

1.  **[Service Publisher](service-publisher/publisher.md)**: A Scala application that simulates IoT sensors. Each instance generates 1000 sensor readings (JSON) per second and publishes them to an MQTT broker.
2.  **Mosquitto MQTT Broker**: Acts as the central messaging hub, facilitating communication between publishers and subscribers.
3.  **[Service Subscriber](service-subscriber/)**: A Scala application that consumes messages from the `sensors/#` wildcard topic. It dynamically creates a dedicated Pekko Actor for each unique sensor topic to maintain state (running sum and last timestamp). DB writes are handled through a pluggable `DatabaseBackend` trait, with the active implementation selected at runtime via `DB_BACKEND`.
4.  **Prometheus**: Scrapes metrics from the containers, the host system, and the **Service Subscriber**.
5.  **Grafana**: Provides a visual dashboard for monitoring container resource usage and application-specific metrics.
6.  **TimescaleDB**: A PostgreSQL extension for high-performance time-series data storage.

## 🚀 Getting Started

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)

### Running the Pipeline

To start the entire stack with 3 simulated sensors (publisher instances):

```bash
docker compose up -d --build --scale publisher=3
```

### Monitoring & Logs

- **Grafana**: Accessible at [http://localhost:3000](http://localhost:3000) without authentication.
    - Pre-provisioned with Prometheus and a "Container Monitoring" dashboard.
- **Prometheus UI**: Accessible at [http://localhost:9090](http://localhost:9090).
- **Subscriber Metrics**: Raw endpoint available at [http://localhost:8081/metrics](http://localhost:8081/metrics).
    - `subscriber_requests_total` — throughput counter.
    - `subscriber_request_latency_milliseconds` — publisher→subscriber latency (p50/p95/p99/p999).
    - `subscriber_e2e_latency_milliseconds` — publisher→DB latency (p50/p95/p99/p999).
    - `subscriber_db_write_latency_milliseconds` — subscriber→DB write latency (p50/p95/p99/p999).
    - `subscriber_sensor_up{device="<name>"}` — per-sensor liveness gauge (1 = ALIVE, 0 = MISSING).
- **Subscriber Logs**:
    ```bash
    docker logs -f subscriber
    ```

## 🔬 Benchmarking

The subscriber's DB write path is decoupled from any specific database through a pluggable `DatabaseBackend` trait. The active backend is selected at startup via the `DB_BACKEND` environment variable, with no recompilation required.

### How `bench.sh` works

`bench.sh` takes a scenario file, sources it as environment variables, starts the full Docker Compose stack with the configured number of publisher instances (`PUBLISHER_COUNT`), and waits for the subscriber's Prometheus endpoint to come up. It then sets up a Python virtual environment under `benchmarking/.venv` (installing `benchmarking/requirements.txt` automatically on first run — no manual setup needed) and hands off to `benchmarking/monitor.py`.

`monitor.py` queries Prometheus directly for the same panels shown on the "Container Monitoring" Grafana dashboard (message rate, the three latency summaries, sensor liveness, container CPU/memory) every `METRICS_INTERVAL` seconds (default: 10) and renders them as a live-updating table. Press `Ctrl+C` to stop: it prints an avg/max summary of the run and saves every raw, timestamped sample to a JSON file under `benchmarking/output/` — so you can revisit or replot a run's data later without re-running the (slow) benchmark. `bench.sh` then runs a clean `docker compose down -v`.

```
=== Benchmark: timescale_batch.env ===
  Publishers  : 5  (~5000 msg/s)
  Batch       : true  (size=100, timeout=1000ms)
  DB pool     : 5 workers
  Backend     : timescaledb

                     Live benchmark metrics
  time      msgs/s  req p50  req p99  e2e p50  e2e p99  db p50  db p99  sensors up/down  cpu (cores)  mem (MB)
 14:22:01    1245    12.1 ms  39.8 ms  28.3 ms  54.1 ms  27.1 ms  32.9 ms      3 / 0          0.06        180.4
 14:22:11    2445    12.3 ms  40.1 ms  27.9 ms  53.8 ms  26.8 ms  33.1 ms      3 / 0          0.07        184.9
```

**Single vs. batch mode.** `bench.sh <scenario>` runs one scenario interactively (stop with `Ctrl+C`). Set `RUN_DURATION` to run it unattended for a fixed number of seconds instead. `bench.sh all` sweeps **every** scenario in `benchmarking/scenarios/` back-to-back: it builds the images once up front, runs each for `RUN_DURATION` seconds (default `60`), writes one JSON per scenario to `benchmarking/output/`, and tears the stack down (`docker compose down -v`) between runs so each starts cold.

To override the poll interval, set `METRICS_INTERVAL` in your scenario file or environment (defaults: `5`s in duration mode, `10`s interactive).

### Running a scenario

```bash
chmod +x bench.sh

# One scenario, interactively (Ctrl+C to stop):
./bench.sh benchmarking/scenarios/timescale_load_p20.env

# One scenario, fixed 60s unattended run:
RUN_DURATION=60 ./bench.sh benchmarking/scenarios/timescale_load_p20.env

# The full OFAT sweep (all 24 scenarios, 60s each, unattended):
./bench.sh all
```

### Available scenarios

All 24 scenarios follow a **one-factor-at-a-time (OFAT)** design: every file changes exactly one variable from a shared **anchor** (`20` publishers ≈ 20k msg/s, pool `20`, batching off, no payload padding), so any measured effect is attributable to that one factor. The batch scenarios share a **batch sub-anchor** (batching on, size `100`, timeout `500`ms) that differs from the anchor only by enabling batching.

| Group | File pattern | Factor swept | Values (**bold** = anchor) |
|-------|--------------|--------------|----------------------------|
| Load | `timescale_load_p{NN}.env` | `PUBLISHER_COUNT` | 4, 8, 16, **20**, 32, 48, 64 |
| Pool | `timescale_pool_{NN}.env` | `DB_POOL_SIZE` | 5, 10, **20**, 50 |
| Payload | `timescale_payload_{N}.env` | `PAYLOAD_PADDING_BYTES` | **0**, 256, 1KB, 10KB |
| Batch size | `timescale_batchsize_{NNN}.env` | `BATCH_SIZE` (batch on) | 20, 50, **100**, 200, 500 |
| Batch timeout | `timescale_batchto_{NNNN}.env` | `BATCH_TIMEOUT_MS` (batch on) | 50, 200, **500**, 1000 |

The no-batch-vs-batch comparison is the anchor (`timescale_load_p20.env`) versus the batch sub-anchor (`timescale_batchsize_100.env`, identical to `timescale_batchto_0500.env`).

### Scenario variables reference

| Variable | Default | Description |
|----------|---------|-------------|
| `PUBLISHER_COUNT` | `1` | Number of publisher containers (`--scale publisher=N`) |
| `DB_BACKEND` | `timescaledb` | Backend implementation to use |
| `DB_POOL_SIZE` | `20` | Connection pool size (HikariCP in non-batch mode; actor pool in batch mode) |
| `BATCH_ENABLED` | `false` | Enable row buffering |
| `BATCH_SIZE` | `100` | Flush when buffer reaches this many rows |
| `BATCH_TIMEOUT_MS` | `1000` | Flush after this many ms even if buffer is not full |
| `PAYLOAD_PADDING_BYTES` | `0` | Extra filler bytes added as a `"padding"` field in each publisher's JSON payload, for payload-size benchmarks |
| `RUN_DURATION` | _(unset)_ | Fixed measurement window in seconds. Set it (or use `bench.sh all`, which defaults it to `60`) for an unattended run; leave unset for an interactive `Ctrl+C` run |
| `METRICS_INTERVAL` | `5` / `10` | Seconds between metric snapshots (defaults to `5` in duration mode, `10` interactive) |

### Supported `DB_BACKEND` values

| Value | Class | Description |
|-------|-------|-------------|
| `timescaledb` (default) | `TimescaleDBBackend` | PostgreSQL/TimescaleDB via HikariCP + JDBC |

### TODO / Planned

- **Warm-up analysis** — runs currently measure from `t=0` including startup. Retain the per-interval time series and analyse the startup transient (latency-vs-time, time-to-steady-state — the JVM ramp vs. BEAM's flat start) separately from the steady-state plateau, rather than folding both into one aggregate.
- **Repetitions (K=3)** — run each scenario 3× with a full teardown between reps and report mean ± stdev, so a runtime difference can be told apart from run-to-run noise. Pilot the spread on one scenario first to confirm K. (Needs a rep tag in `monitor.py`'s output filename and a rep loop in `bench.sh`.)
- **Post-run report** — after a full sweep, aggregate the per-scenario JSON into a **CSV** (one row per runtime × scenario) and write a **Markdown** report (load-saturation curves, OFAT plots, batch crossover, interpretation) for lifting into the thesis. Excel only as an optional throwaway export.

## 🧠 Deep Dive: Pekko Executors

### Fork-Join vs. Virtual Threads

The project explores different execution models for Pekko Actors:

#### Fork-Join Executor (Default)
In the default implementation, actors are dispatched on a `fork-join-executor`. Actors are treated as tasks pushed onto a deque and executed on a fixed pool of platform threads.
- **Pros**: Highly efficient for non-blocking workloads.
- **Cons**: If an actor performs a blocking I/O operation, it stalls the underlying platform thread, potentially leading to thread starvation.

#### Virtual Threads (Project Loom)
Pekko 1.2.0+ and Java 21/24 introduce support for `virtual-thread-executor`. Virtual threads are lightweight, runtime-managed threads (similar to Goroutines in Go).
- **Behavior**: When a virtual thread blocks, it is unpinned from its carrier platform thread, allowing other virtual threads to continue execution.
- **Advantage**: Allows writing simple, blocking code while maintaining the scalability of asynchronous systems.

The current implementation uses the **Fork-Join Executor**. To experiment with Virtual Threads, the `application.conf` can be adjusted to use the `pekko.dispatch.VirtualThreadExecutorConfigurator`.

## 🛠️ Tech Stack

- **Language**: Scala 3
- **Concurrency**: Pekko (Actors & Streams)
- **Messaging**: MQTT (Mosquitto / via Alpakka, Pekko Connectors)
- **JSON**: Circe
- **Observability**: Prometheus & Grafana
- **Database**: Pluggable backends (TimescaleDB default)
- **Deployment**: Docker & Docker Compose
