package com.publisher

import org.apache.pekko.Done
import org.apache.pekko.actor.ActorSystem
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

/**
 * Main entry point for the Publisher service.
 * This service simulates an IoT sensor by generating periodic data and publishing it to MQTT.
 */
object Main {

  /**
   * Generates a JSON payload representing a sensor reading.
   * @param deviceName Unique identifier for the simulated device.
   * @return JSON string containing device name, timestamp, and a random value.
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

    // Retrieve the container's hostname to create unique client IDs and topics
    val hostname = java.net.InetAddress.getLocalHost.getHostName

    // Configure MQTT connection settings for the Mosquitto broker
    val connectionSettings = MqttConnectionSettings(
      "tcp://mosquitto:1883",
      s"test-publisher-$hostname",
      new MemoryPersistence
    )

    // Create an MQTT Sink to handle publishing messages
    // MqttQoS.AtLeastOnce ensures that messages are delivered reliably
    val sink: Sink[MqttMessage, Future[Done]] =
      MqttSink(connectionSettings, MqttQoS.AtLeastOnce)

    // Define the Pekko Stream source:
    // 1. Source.tick: Generates a signal every 100ms
    // 2. map: Transforms the signal into a JSON data payload
    // 3. wireTap: Side-effecting operation to log the payload to stdout
    // 4. map: Wraps the payload into an MqttMessage with a unique topic per sensor
    lazy val mqttSource =
      Source
        .tick(1.second, 100.millis, ())
        .map(_ => data(s"sensor$hostname"))
        .wireTap(payload => println(s"Payload created: $payload"))
        .map { payload =>
          MqttMessage(s"sensors/sensor$hostname", ByteString(payload))
        }

    // Materialize and run the stream connecting the source to the MQTT sink
    val control = mqttSource.runWith(sink)

    // Graceful shutdown logic: Wait for the stream (which is infinite in this case) 
    // or wait for termination signals.
    Await.result(control, 5.seconds)
    system.terminate()
    Await.result(system.whenTerminated, 5.seconds)
  }

}
