# Service Subscriber

The **Service Subscriber** is a Scala-based application responsible for ingesting, processing, and storing the real-time stream of telemetry data produced by the publishers. It is built using [Pekko Streams](https://pekko.apache.org/docs/pekko/current/stream/index.html) and [Pekko Actors](https://pekko.apache.org/docs/pekko/current/actor/index.html).

## ⚙️ Core Functionality

1. **Wildcard Ingestion:**
   The Subscriber connects to the Mosquitto MQTT Broker and subscribes to the wildcard topic `sensors/#`. This allows a single subscriber instance to receive telemetry data from every publisher on the network.

2. **Dynamic Actor Creation:**
   Instead of processing all messages in a single bottleneck process, the Subscriber utilizes the Actor Model. For every unique sensor topic it discovers, it spawns a dedicated **TopicActor**. This actor manages the state (cumulative sum) and processing for that specific sensor.

3. **Data Transformation & Storage:**
   The Subscriber parses incoming JSON payloads using [Circe](https://circe.github.io/circe/) and extracts the relevant information. It then persists this data into **TimescaleDB** using [HikariCP](https://github.com/brettwooldridge/HikariCP) for efficient connection pooling.

4. **Metrics and Observability:**
   The service exposes a metrics endpoint for [Prometheus](https://prometheus.io/) to scrape, providing visibility into the pipeline's performance.

## 🏗️ Architecture

The application is built around the **Pekko** ecosystem:

- **Pekko Actor System**: The root for managing all concurrent processes and actors.
- **MQTT Connector**: Utilizes Alpakka MQTT for robust connectivity to the Mosquitto broker.
- **Topic Actors**: Dynamically spawned actors that handle state management and processing per sensor topic.
- **Connection Pooling**: Uses HikariCP to manage connections to TimescaleDB, ensuring optimal throughput.

## 📊 Metrics Tracking

The service exposes the following Prometheus metrics on port `8081` (raw endpoint: `http://localhost:8081/metrics`):

- `subscriber_requests_total`: Total count of MQTT messages processed.
- `subscriber_request_latency_milliseconds`: Latency (processing time − sensor timestamp) with quantiles (p50, p95, p99, p999).
- `subscriber_e2e_latency_milliseconds`: End-to-end latency (DB ack − sensor timestamp) with quantiles (p50, p95, p99, p999).
- `subscriber_sensor_up{device="<name>"}`: Per-sensor liveness gauge — `1` = ALIVE, `0` = MISSING. Updated every heartbeat.
- **JVM Metrics**: Standard metrics for garbage collection, memory usage, and thread counts.

## ⚙️ Configuration

The service uses the following environment variables for database connectivity:
- `DB_URL`: JDBC URL (default: `jdbc:postgresql://localhost:5432/epu`)
- `DB_USER`: Database user (default: `postgres`)
- `DB_PASSWORD`: Database password (default: `postgres`)

## 🔍 Missing Sensor Detection

The Subscriber implements a heartbeat-based missing sensor detection system:

- Every **5 seconds**, the main stream scheduler sends a `"heartbeat"` message to all active `TopicActor` instances.
- Each actor compares its `lastSeen` timestamp (captured using the subscriber's local epoch seconds) against the current time.
- If a sensor has not sent data within the last second, its status transitions to `MISSING`; otherwise it is `ALIVE`.
- State changes are logged via the actor logger (`log.warning`) and persisted asynchronously to the `sensor_status` table in TimescaleDB, but only when the status actually changes to avoid unnecessary writes.

## 📡 MQTT Quality of Service

The subscriber connects with **QoS 0 (At Most Once)** via `MqttQoS.AtMostOnce`. This provides fire-and-forget delivery with no acknowledgment overhead, prioritizing throughput and low latency over guaranteed delivery.
