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

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

object Main {
  def main(args: Array[String]): Unit = {
    implicit val system: ActorSystem = ActorSystem("publisher")
    implicit val ec = system.dispatcher

    // Create connection settings
    val connectionSettings = MqttConnectionSettings(
      "tcp://mosquitto:1883",
      "test-publisher",
      new MemoryPersistence
    )

    // Create a stream processor
    val sink: Sink[MqttMessage, Future[Done]] =
      MqttSink(connectionSettings, MqttQoS.AtLeastOnce)

    // Create a source of infinite messages
    val tickerSource = Source.tick(1.second, 100.millis, "payload")
    val mqttSource = tickerSource
      .wireTap(payload => println(s"Payload created: $payload"))
      .map { payload =>
        MqttMessage("your/topic", ByteString(payload))
      }

    // Connect to the existing sink
    val control = mqttSource.runWith(sink)

    // sys.addShutdownHook(system.terminate())
  }
}
