package com.subscriber

import org.apache.pekko.actor.{ActorRef, ActorSystem, Props}
import org.apache.pekko.stream.connectors.mqtt.MqttConnectionSettings
import org.apache.pekko.stream.connectors.mqtt.MqttQoS
import org.apache.pekko.stream.connectors.mqtt.scaladsl.MqttSource
import org.apache.pekko.stream.connectors.mqtt.MqttSubscriptions
import org.apache.pekko.stream.scaladsl.Sink
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence

import scala.collection.concurrent.TrieMap
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.DurationInt

/** Main entry point for the Subscriber service. Subscribes to wildcard MQTT
  * topics and dispatches messages to dedicated actors.
  */
object Main {
  def main(args: Array[String]): Unit = {
    // Initialize Prometheus metrics server
    Metrics.init(8081)

    // Initialize Pekko system and context
    implicit val system: ActorSystem = ActorSystem("subscriber")
    implicit val ec = system.dispatcher

    // Configure connection to the MQTT broker
    val mqttConnectionSettings = MqttConnectionSettings(
      "tcp://mosquitto:1883",
      "test-subscriber",
      new MemoryPersistence
    )

    // Define wildcard subscription: "sensors/#" matches all sub-topics under sensors
    val wildcardTopic = "sensors/#"
    val subscriptions = MqttSubscriptions(
      Map(wildcardTopic -> MqttQoS.AtMostOnce)
    )

    // Create an MQTT source that emits messages arriving on the subscribed topics
    // Increased bufferSize to 1024 to handle higher throughput
    val mqttSource =
      MqttSource.atMostOnce(
        mqttConnectionSettings.withClientId("test-subscriber"),
        subscriptions,
        bufferSize = 1024
      )

    // Thread-safe map to store and look up actors based on the MQTT topic name.
    val actors = TrieMap.empty[String, ActorRef]

    // Execute the stream processing pipeline
    mqttSource.runWith(Sink.foreach { msg =>
      val topic = msg.topic
      val actorRef = actors.getOrElseUpdate(
        topic,
        system.actorOf(Props(new TopicActor(topic)))
      )
      actorRef ! msg
    })

    // Heartbeat every 5 seconds to check for missing sensors
    system.scheduler.scheduleWithFixedDelay(5.seconds, 5.seconds) { () =>
      actors.values.foreach(_ ! "heartbeat")
    }

    // Register a shutdown hook
    sys.addShutdownHook {
      Metrics.stop()
      system.terminate()
      Await.result(system.whenTerminated, 5.seconds)
    }
  }
}

