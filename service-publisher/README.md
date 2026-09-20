# Service Publisher

The Service Publisher is a Scala application that simulates an IoT sensor. It generates the data
for the pipeline: a steady stream of sensor readings, published to the message broker.

## Core Functionality

1. **Simulation of an IoT device.**
   The service is built on
   [Pekko Streams](https://pekko.apache.org/docs/pekko/current/stream/index.html). It derives a
   sensor identifier from the hostname of its container and produces telemetry the way a hardware
   sensor in the field does.

2. **Data generation.**
   The service produces 1000 JSON payloads per second, throttled to that rate by Pekko Streams
   backpressure. The payload is the shared wire format of the benchmark. It is byte-for-byte
   identical to what the Erlang arm publishes for the same reading. `PAYLOAD_PADDING_BYTES`
   therefore adds its filler to the same base payload in both arms:

   ```json
   {"device_name":"sensor<hostname>","timestamp":"2026-08-21T12:34:56.123456Z","value":7}
   ```

   - `device_name`: The identifier of the sensor, derived from the container hostname.
   - `timestamp`: RFC 3339 in UTC with exactly six fractional digits, formatted by `DateTimeFormatterBuilder().appendInstant(6)`. The width is fixed, unlike `Instant.toString`, which drops trailing zeros and changes the payload size from message to message.
   - `value`: A random integer from 1 to 10, published as a JSON number and not as a quoted string.
   - `padding`: Present only when `PAYLOAD_PADDING_BYTES` is above 0. It is the letter `x` repeated that many times, and it is always the last field.

   The payload is compact, with no whitespace after a colon or a comma, and the field order is
   fixed. Both belong to the shared format and are not incidental formatting.

3. **MQTT publishing.**
   The service connects to the Mosquitto MQTT broker, `tcp://mosquitto:1883` by default. It
   publishes each message to a topic of its own device, such as `sensors/sensor<hostname>`.

## Architecture

The publisher runs on Pekko Streams and Pekko Connectors MQTT (Alpakka). The Pekko actor system is
the runtime that carries the concurrency. Pekko Streams carries the data from generation to
publishing, and its backpressure is what holds the rate steady. Logging goes through Pekko Logging
with Logback.

## Scaling

The publisher derives its `device_name` from the system hostname, so it holds no state of its own
and scales by replication. Simulate a fleet of sensors by scaling the service with Docker Compose:

```bash
docker compose up -d --scale publisher=100
```

That starts 100 isolated sensor instances, all feeding the pipeline at the same time.
