# Service Subscriber

The **Service Subscriber** is a Scala-based application responsible for ingesting, processing, and storing the real-time stream of telemetry data produced by the publishers. It is built using [Pekko Streams](https://pekko.apache.org/docs/pekko/current/stream/index.html) and [Pekko Actors](https://pekko.apache.org/docs/pekko/current/actor/index.html).

## ⚙️ Core Functionality

1. **Wildcard Ingestion:**
   The Subscriber connects to the Mosquitto MQTT Broker and subscribes to the wildcard topic `sensors/#`. This allows a single subscriber instance to receive telemetry data from every publisher on the network.

2. **Dynamic Actor Creation:**
   Instead of processing all messages in a single bottleneck process, the Subscriber utilizes the Actor Model. For every unique sensor topic it discovers, it spawns a dedicated **TopicActor**. This actor manages the state (cumulative sum) and processing for that specific sensor.

3. **Data Transformation & Storage:**
   The Subscriber parses incoming JSON payloads using [Circe](https://circe.github.io/circe/) and extracts the relevant information. It then persists this data into **TimescaleDB** via the pluggable `DatabaseBackend`. Depending on `BATCH_ENABLED`, writes go through a HikariCP connection pool (non-batch) or a pool of `DbWriterActor` instances (batch). Latency recording is performed inside the backend, not in `TopicActor`.

4. **Metrics and Observability:**
   The service exposes a metrics endpoint for [Prometheus](https://prometheus.io/) to scrape, providing visibility into the pipeline's performance.

## 📨 Message Flow

```mermaid
flowchart TD
    A[MQTT Broker] --> B["Alpakka MqttSource\nbufferSize=1024"]
    B --> C["Sink.foreach\nTrieMap lookup / getOrElseUpdate TopicActor"]
    C --> D[TopicActor.receive MqttMessage]
    D --> E["Circe JSON decode\nPrometheus: requests_total (ingest) + subscriber latency"]
    E --> F["db.insertData\npublisherEpochUs, subscriberReceiveUs"]
    F --> G[TimescaleDBBackend.insertData]
    G --> H{BATCH_ENABLED}
    H -->|false| I["HikariCP: acquire connection\nJDBC prepareStatement + execute\nRecord committed_total (+1) + e2e + db_write inline"]
    H -->|true| J["round-robin select DbWriterActor\nactor ! InsertRow\nFuture.successful immediately"]
    I --> K[(TimescaleDB)]
    J --> L["DbWriterActor buffers row\nflush on batchSize or timeout\npre-prepared unnest INSERT (3 array params)\nRecord committed_total (+batch size) + latency per row at flush"]
    L --> K
```

Heartbeat path (every 5 seconds via `system.scheduler`):

```mermaid
flowchart TD
    A["system.scheduler\nevery 5 seconds"] --> B["actors.values.foreach\n_ ! heartbeat"]
    B --> C["TopicActor.receive(heartbeat)"]
    C --> D["determine status:\nMISSING if never seen or lastSeen > 1s ago\nALIVE otherwise"]
    D --> E{status changed?}
    E -->|yes| F["db.insertStatus\nlog.warning"]
    E -->|no| G["Metrics.sensorUp.set\n1 = ALIVE, 0 = MISSING"]
    F --> G
```

## 🏗️ Architecture

The application is built around the **Pekko** ecosystem:

- **Pekko Actor System**: The root for managing all concurrent processes and actors.
- **MQTT Connector**: Utilizes Alpakka MQTT for robust connectivity to the Mosquitto broker. Messages arrive via a Pekko Stream (`MqttSource.atMostOnce`) and are dispatched to actors via `Sink.foreach`.
- **Clock**: The single definition of a microsecond wall-clock stamp; every latency measurement reads the clock through it. The Erlang arm gets the same from `os:system_time(microsecond)` directly.
- **Topic Actors (`TopicActor`)**: Dynamically spawned actors that handle state management and processing per sensor topic. They receive the `DatabaseBackend` instance via constructor injection, so the backend can be swapped without modifying the actor. `TopicActor` records the ingest count and subscriber-receive latency; **the committed-rows count and the e2e/db_write latency recording are delegated to the backend** so they work correctly in both batch and non-batch modes.
- **Actor Registry**: A `TrieMap[String, ActorRef]` in `Main` maps topic strings to their actor. `getOrElseUpdate` is used for lock-free creation on first sight of a new topic.
- **DB Backend Trait (`DatabaseBackend`)**: A trait defining the pluggable interface for database writes — `insertData`, `insertStatus`, and `close`. `insertData` takes `publisherEpochUs` and `subscriberReceiveUs` (microseconds, matching the publisher's stamp precision). `publisherEpochUs` is both the value written to the `Timestamp` column — via `Clock.toOffsetDateTime` in whichever backend binds it — and the start of the e2e measurement, so the reading's instant crosses the interface once, in one representation. The backend computes and records e2e and db_write latency itself (at Future completion in non-batch mode, or at batch flush time in batch mode). All implementations must be thread-safe, as the single instance is shared across all `TopicActor` instances running concurrently. The active backend is selected at JVM startup by the `Database` object via the `DB_BACKEND` environment variable.
- **DB Backend Selector (`Database`)**: An object with a `def backend(implicit system: ActorSystem)` that reads `DB_BACKEND` (default: `"timescaledb"`) and instantiates the matching backend. The `ActorSystem` implicit is required by backends that create child actors. The instance is passed to every `TopicActor` at construction time.
- **TimescaleDB Backend (`TimescaleDBBackend`)**: The concrete implementation of `DatabaseBackend` for TimescaleDB. Takes an implicit `ActorSystem` and reads `BATCH_ENABLED`, `BATCH_SIZE`, `BATCH_TIMEOUT_MS`, and `DB_POOL_SIZE` at construction time.
  - **Non-batch mode** (`BATCH_ENABLED=false`): creates a HikariCP pool of `DB_POOL_SIZE` connections (default 20) with prepared-statement caching. Each `insertData` call acquires a connection, executes the statement, releases it, then records e2e and db_write latency inline. Writes run on `blocking-io-dispatcher`.
  - **Batch mode** (`BATCH_ENABLED=true`): creates a pool of `DB_POOL_SIZE` `DbWriterActor` instances. `insertData` sends an `InsertRow` message to a round-robin selected actor and returns `Future.successful(())` immediately. Latency is recorded per message at flush time inside the actor.
  - **Status writes**: `insertStatus` shares the data path's connections in both modes — the HikariCP pool in non-batch mode, a round-robin `DbWriterActor` (via the same counter as data) in batch mode. `DB_POOL_SIZE` is therefore the total PostgreSQL connection count, matching the Erlang arm, where `insert_status` likewise routes over the shared worker pool. A dedicated status pool would add two connections the Erlang side does not have, which distorts the pool sweep most at its smallest value.
- **DbWriterActor**: A Pekko actor that owns one JDBC connection and one row buffer. It runs on `blocking-io-dispatcher`. When it receives an `InsertRow` message it appends to the buffer and starts a single-fire timer if the buffer was previously empty. The buffer is flushed when it reaches `batchSize` rows or the `BATCH_TIMEOUT_MS` timer fires, as a single `INSERT ... SELECT unnest(?::text[]), unnest(?::int4[]), unnest(?::timestamptz[])` — three array parameters, so the SQL text is fixed and the statement is prepared once at construction and reused at any batch size. This mirrors the pre-parsed unnest statement in `db_backend_timescaledb.erl`; a `VALUES` list would instead rebuild and re-prepare a different statement on every flush, making the batching factors measure the JDBC driver rather than the runtime. An `InsertStatus` message is executed immediately rather than buffered. After a successful flush, e2e and db_write latency are recorded for every row in the flushed batch. On `postStop` any remaining buffered rows are flushed and the connection is closed.

## 📊 Metrics Tracking

The service exposes the following Prometheus metrics on port `8081` (raw endpoint: `http://localhost:8081/metrics`):

- `subscriber_requests_total`: Total count of MQTT messages **ingested** — incremented on receive, before the row reaches the database.
- `subscriber_committed_total`: Total rows **committed** to the database — incremented on the DB write ack (by the batch size in batching mode, by 1 otherwise). Compare against `subscriber_requests_total`: the two track each other while the DB keeps up, and diverge once the write path saturates.
- `subscriber_request_latency_milliseconds`: Latency (processing time − sensor timestamp), as a histogram.
- `subscriber_e2e_latency_milliseconds`: End-to-end latency (DB ack − sensor timestamp), as a histogram.
- `subscriber_db_write_latency_milliseconds`: Latency from subscriber receive to DB write ack, as a histogram.
- `subscriber_sensor_up{device="<name>"}`: Per-sensor liveness gauge — `1` = ALIVE, `0` = MISSING. Updated every heartbeat.
- **JVM Metrics**: Standard metrics for garbage collection, memory usage, and thread counts.

All three latency metrics are Prometheus **histograms** over one bucket list (39 finite bounds from
0.05 ms to 60 s) that is byte-identical to the other repo's, so both arms bucket the same
observations the same way and quantiles are comparable by construction. Quantiles are computed at
query time with `histogram_quantile()`, which means any quantile can be recomputed over any window
after the fact — that is what lets the benchmark harness report steady state separately from the
startup transient.

> **Reading the quantiles.** `histogram_quantile()` interpolates linearly inside a bucket, so a
> quantile is accurate to at most the width of the bucket it falls in — bounded, known in advance,
> and bounded in *milliseconds*. A quantile falling in the `+Inf` bucket returns the highest finite
> bound, so a saturated scenario reads as clamped at 60,000 ms. Both are covered by the `_sum` /
> `_count` pair, which gives an exact mean that is neither quantized nor clamped; the benchmark
> report carries it as a column beside every quantile for exactly this reason.

`Metrics.recordCommit` / `recordCommitBatch` are the only places a row is counted as committed and
its latencies observed against the DB ack clock. Each backend used to hand-roll that with its own
ack stamp, so one that drifted produced a run that looked healthy and was silently non-comparable.

## ⚙️ Configuration

The service uses the following environment variables:

**Database connectivity:**
- `DB_BACKEND`: Backend implementation (default: `timescaledb`)
- `DB_URL`: JDBC URL (default: `jdbc:postgresql://timescaledb:5432/epu`)
- `DB_USER`: Database user (default: `postgres`)
- `DB_PASSWORD`: Database password (default: `postgres`)

**Write mode (`TimescaleDBBackend`):**
- `BATCH_ENABLED`: Enable row buffering / batch inserts (default: `false`)
- `BATCH_SIZE`: Flush when the buffer reaches this many rows (default: `100`)
- `BATCH_TIMEOUT_MS`: Flush after this many ms even if the buffer is not full (default: `1000`)
- `DB_POOL_SIZE`: HikariCP pool size in non-batch mode; number of `DbWriterActor` instances in batch mode (default: `20`)

## 🔍 Missing Sensor Detection

The Subscriber implements a heartbeat-based missing sensor detection system:

- Every **5 seconds**, the main stream scheduler sends a `"heartbeat"` message to all active `TopicActor` instances.
- Each actor compares its `lastSeen` timestamp (captured using the subscriber's local epoch seconds) against the current time.
- If a sensor has not sent data within the last second, its status transitions to `MISSING`; otherwise it is `ALIVE`.
- State changes are logged via the actor logger (`log.warning`) and persisted asynchronously to the `sensor_status` table in TimescaleDB, but only when the status actually changes to avoid unnecessary writes.

## 📡 MQTT Quality of Service

The subscriber connects with **QoS 0 (At Most Once)** via `MqttQoS.AtMostOnce`. This provides fire-and-forget delivery with no acknowledgment overhead, prioritizing throughput and low latency over guaranteed delivery.
