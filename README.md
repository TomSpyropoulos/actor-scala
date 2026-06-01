# IoT Data Pipeline (Scala & Pekko)

A high-performance, containerized IoT data pipeline implemented using **Scala**, **Pekko Actors**, and **Pekko Streams**. This project demonstrates how to build a scalable messaging system that handles real-time sensor data with backpressure and efficient resource management.

## 🏗️ System Architecture

The system consists of the following components:

1.  **[Service Publisher](service-publisher/publisher.md)**: A Scala application that simulates IoT sensors. Each instance generates 1000 sensor readings (JSON) per second and publishes them to an MQTT broker.
2.  **Mosquitto MQTT Broker**: Acts as the central messaging hub, facilitating communication between publishers and subscribers.
3.  **[Service Subscriber](service-subscriber/subscriber.md)**: A Scala application that consumes messages from the `sensors/#` wildcard topic. It dynamically creates a dedicated Pekko Actor for each unique sensor topic to maintain state (running sum and last timestamp).
5.  **Prometheus**: Scrapes metrics from the containers, the host system, and the **Service Subscriber**.
6.  **Grafana**: Provides a visual dashboard for monitoring container resource usage and application-specific metrics.
7.  **TimescaleDB**: A PostgreSQL extension for high-performance time-series data storage.


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
- **Subscriber Logs**: View the aggregated state for each sensor:
    ```bash
    docker logs -f subscriber
    ```

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

The current implementation uses the **Fork-Join Executor**. To experiment with Virtual Threads, the `application.conf` (or actor system configuration) can be adjusted to use the `pekko.dispatch.VirtualThreadExecutorConfigurator`.

## 🛠️ Tech Stack

- **Language**: Scala 3
- **Concurrency**: Pekko (Actors & Streams)
- **Messaging**: MQTT (Mosquitto / via Alpakka,Pekko Connectors)
- **JSON**: Circe
- **Observability**: Prometheus & Grafana
- **Database**: TimescaleDB
- **Deployment**: Docker & Docker Compose
