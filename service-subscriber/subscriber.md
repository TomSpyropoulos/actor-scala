# Service Subscriber

The **Service Subscriber** is a Scala application built with [Pekko Streams](https://pekko.apache.org/docs/pekko/current/stream/index.html) and [Pekko Actors](https://pekko.apache.org/docs/pekko/current/actor/index.html). It consumes messages from an MQTT broker, records metrics, and stores the data in [TimescaleDB](https://www.timescale.com/).

## Functionality

- **MQTT Subscription**: Subscribes to the wildcard topic `sensors/#` to receive data from all sensors.
- **Actor per Topic**: Dynamically creates a `TopicActor` for each unique MQTT topic it discovers. This actor manages the state (cumulative sum) and processing for that specific sensor.
- **Data Persistence**: Parses JSON payloads and inserts them into TimescaleDB using [HikariCP](https://github.com/brettwooldridge/HikariCP) for efficient connection pooling.
- **Prometheus Monitoring**: Exposes a metrics endpoint for [Prometheus](https://prometheus.io/) to scrape.

## Technical Stack

- **Language**: Scala
- **Runtime**: Pekko Actor System & Pekko Streams
- **JSON Parsing**: [Circe](https://circe.github.io/circe/)
- **Database**: TimescaleDB (PostgreSQL)
- **Monitoring**: Prometheus Java Client
- **Logging**: Pekko Logging with Logback

## Metrics

The service exposes the following Prometheus metrics on port `8081`:

- `subscriber_requests_total`: Total count of MQTT messages processed.
- `subscriber_request_latency_milliseconds`: Latency (processing time - sensor timestamp) with quantiles (p50, p95, p99).
- Standard JVM metrics (GC, memory, threads).

## Configuration

Environment variables used for database connection:
- `DB_URL`: JDBC URL (default: `jdbc:postgresql://localhost:5432/epu`)
- `DB_USER`: Database user (default: `postgres`)
- `DB_PASSWORD`: Database password (default: `postgres`)
