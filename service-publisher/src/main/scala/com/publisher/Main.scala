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
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}

// Publisher entry point: simulates an IoT sensor by generating periodic readings and publishing them to MQTT
object Main {

  // Fixed-width fraction, unlike Instant.toString, which elides trailing zeros and varies the payload size
  private val IsoMicros: DateTimeFormatter =
    new DateTimeFormatterBuilder().appendInstant(6).toFormatter

  // Builds a JSON sensor reading; paddingBytes > 0 appends a filler "padding" field for payload-size benchmarks.
  // These exact bytes are the shared wire format -- byte-identical to handle_info(publish_tick, ...) in
  // actor-erlang/service_publisher/src/service_publisher_srv.erl; keep the two in sync.
  def data(deviceName: String, paddingBytes: Int): String = {
    val timestampz = IsoMicros.format(Instant.now())
    val value = Random().nextInt(10) + 1
    val padding = if (paddingBytes > 0) s""","padding":"${"x" * paddingBytes}"""" else ""
    s"""{"device_name":"$deviceName","timestamp":"$timestampz","value":$value$padding}"""
  }

  // Wires the Pekko stream that generates readings and publishes them to MQTT, then runs until the JVM exits
  def main(args: Array[String]): Unit = {
    // Initialize Pekko ActorSystem and ExecutionContext
    implicit val system: ActorSystem = ActorSystem("publisher")
    implicit val ec = system.dispatcher
    val log = Logging(system, "publisher")

    // Retrieve the container's hostname to create unique client IDs and topics
    val hostname = java.net.InetAddress.getLocalHost.getHostName
    val paddingBytes = sys.env.getOrElse("PAYLOAD_PADDING_BYTES", "0").toInt

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
    // 1. Source.repeat + throttle(1000, 1.second): generates 1000 signals per second
    // 2. map: Transforms the signal into a JSON data payload
    // 3. map: Wraps the payload into an MqttMessage
    //
    // Deliberately no per-message logging in this stream. A wireTap logging each payload
    // interpolated the whole payload (padding included) once per message at 1000 msg/s, and
    // the Erlang publisher has no equivalent -- it was a one-sided cost that made the two
    // benchmark arms incomparable. Keep any debugging output out of the hot path.
    lazy val mqttSource =
      Source
        .repeat(())
        .throttle(1000, 1.second)
        .map(_ => data(s"sensor$hostname", paddingBytes))
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
