# Service Publisher

The **Service Publisher** is a Scala-based application designed to simulate the behavior of IoT sensors. It acts as the primary data generator for the IoT data pipeline, producing a steady stream of sensor readings and dispatching them to the central messaging hub.

## ⚙️ Core Functionality

1. **Simulation of IoT Devices:** 
   Built using [Pekko Streams](https://pekko.apache.org/docs/pekko/current/stream/index.html), it simulates real-world hardware sensors by generating unique identifiers based on the container's hostname.

2. **Data Generation:**
   The service produces **1000 JSON payloads per second** (throttled at 1000/sec via Pekko Streams backpressure). Each payload contains: The payload is constructed with the following structure:
   ```json
   {
     "device_name": "sensor<hostname>",
     "timestamp": "2023-10-27T10:00:00Z",
     "value": 7
   }
   ```

3. **MQTT Publishing:**
   The service connects to the **Mosquitto MQTT Broker** (defaulting to `tcp://mosquitto:1883`) and publishes messages to a device-specific topic (e.g., `sensors/sensor<hostname>`).

## 🏗️ Architecture

The Publisher utilizes **Pekko Streams** and **Pekko Connectors MQTT (Alpakka)**:
- **Pekko Actor System**: Provides the underlying runtime for managing concurrency.
- **Pekko Streams**: Manages the flow of data from generation to publishing, ensuring backpressure and efficient resource usage.
- **Logging**: Uses Pekko Logging with **Logback** for consistent observability.

## 🚀 Scaling

Because each publisher generates its `device_name` dynamically using the system hostname, it is completely stateless and inherently scalable. You can easily simulate a massive fleet of sensors by scaling the service via Docker Compose:

```bash
docker compose up -d --scale publisher=100
```

This will spin up 100 isolated sensor instances, all feeding data concurrently into the pipeline.
