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
object Main {
  def main(args: Array[String]): Unit = {
    implicit val system: ActorSystem = ActorSystem("publisher")
    implicit val ec = system.dispatcher

    // Create connection settings
    val connectionSettings = MqttConnectionSettings(
      "tcp://localhost:1883",
      "test-publisher",
      new MemoryPersistence
    )

    // Create a stream processor
    val sink: Sink[MqttMessage, Future[Done]] =
      MqttSink(connectionSettings, MqttQoS.AtLeastOnce)

    // Create a source of infinite messages
    lazy val tickerSource = Source.tick(1.second, 100.millis, "payload")
    lazy val mqttSource = tickerSource
      .wireTap(payload => println(s"Payload created: $payload"))
      .map { payload =>
        MqttMessage("your/topic", ByteString(payload))
      }

    // Connect to the existing sink
    val control = mqttSource.runWith(sink)

    // Wait for the stream to finish, then terminate the ActorSystem
    Await.result(control, 5.seconds)
    system.terminate()
    Await.result(system.whenTerminated, 5.seconds)
  }
}
