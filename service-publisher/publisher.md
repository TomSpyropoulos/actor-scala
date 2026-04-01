# Service Publisher

The **Service Publisher** is a Scala application built with [Pekko Streams](https://pekko.apache.org/docs/pekko/current/stream/index.html) that simulates an IoT sensor device. It generates periodic data and publishes it to an MQTT broker.

## Functionality

- **Data Generation**: Produces a JSON payload every second.
- **Payload Structure**:
  ```json
  {
    "device_name": "sensor<hostname>",
    "timestamp": "2023-10-27T10:00:00Z",
    "value": 7
  }
  ```
- **MQTT Publishing**: Connects to the Mosquitto broker at `tcp://mosquitto:1883` and publishes to the topic `sensors/sensor<hostname>`.
- **Unique Identification**: Uses the container's hostname to ensure unique client IDs and sensor names.

## Technical Stack

- **Language**: Scala
- **Runtime**: Pekko Actor System & Pekko Streams
- **MQTT Client**: Pekko Connectors MQTT (Alpakka)
- **Logging**: Pekko Logging with Logback

## Configuration

The service is configured to connect to `mosquitto` on port `1883`. In a Docker environment, it uses the service name `mosquitto` defined in `docker-compose.yaml`.
