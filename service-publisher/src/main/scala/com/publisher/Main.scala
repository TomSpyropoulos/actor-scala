package com.publisher

import org.apache.pekko.Done
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.event.Logging
import org.apache.pekko.stream.connectors.mqtt.MqttConnectionSettings
import org.apache.pekko.stream.connectors.mqtt.MqttMessage
import org.apache.pekko.stream.connectors.mqtt.MqttQoS
import org.apache.pekko.stream.connectors.mqtt.scaladsl.MqttSink
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence

import scala.concurrent.{Future, Await}
import scala.concurrent.duration.DurationInt
import scala.util.Random
import java.time.Instant

/** Main entry point for the Publisher service. This service simulates an IoT
  * sensor by generating periodic data and publishing it to MQTT.
  */
object Main {

  /** Generates a JSON payload representing a sensor reading.
    * @param deviceName
    *   Unique identifier for the simulated device.
    * @return
    *   JSON string containing device name, timestamp, and a random value.
    */
  def data(deviceName: String): String = {
    val timestampz = Instant.now().toString
    val value = Random().nextInt(10) + 1
    s"""{"device_name": "$deviceName", "timestamp":"$timestampz", "value": $value}"""
  }

  def main(args: Array[String]): Unit = {
    // Initialize Pekko ActorSystem and ExecutionContext
    implicit val system: ActorSystem = ActorSystem("publisher")
    implicit val ec = system.dispatcher
    val log = Logging(system, "publisher")

    // Retrieve the container's hostname to create unique client IDs and topics
    val hostname = java.net.InetAddress.getLocalHost.getHostName

    // Configure MQTT connection settings for the Mosquitto broker
    val connectionSettings = MqttConnectionSettings(
      "tcp://mosquitto:1883",
      s"test-publisher-$hostname",
      new MemoryPersistence
    )

    // Create an MQTT Sink to handle publishing messages
    val sink: Sink[MqttMessage, Future[Done]] =
      MqttSink(connectionSettings, MqttQoS.AtMostOnce)

    // Define the Pekko Stream source:
    // 1. Source.tick: Generates a signal every 1ms
    // 2. map: Transforms the signal into a JSON data payload
    // 3. wireTap: Asynchronously log the payload (only if debug is enabled to save perf)
    // 4. map: Wraps the payload into an MqttMessage
    lazy val mqttSource =
      Source
        .repeat(())
        .throttle(1000, 1.second)
        .map(_ => data(s"sensor$hostname"))
        .wireTap(payload =>
          log.info(s"Payload created: $payload")
        )
        .map { payload =>
          MqttMessage(s"sensors/sensor$hostname", ByteString(payload))
        }

    // Materialize and run the stream connecting the source to the MQTT sink
    val control = mqttSource.runWith(sink)

    // Graceful shutdown logic
    sys.addShutdownHook {
      log.info("Shutting down publisher...")
      system.terminate()
      Await.result(system.whenTerminated, 5.seconds)
    }

    Await.result(system.whenTerminated, scala.concurrent.duration.Duration.Inf)
  }

}
