package com.subscriber

import org.apache.pekko.actor.{Actor, ActorRef, ActorSystem, Props}
import org.apache.pekko.stream.connectors.mqtt.MqttConnectionSettings
import org.apache.pekko.stream.connectors.mqtt.MqttMessage
import org.apache.pekko.stream.connectors.mqtt.MqttQoS
import org.apache.pekko.stream.connectors.mqtt.scaladsl.MqttSource
import org.apache.pekko.stream.connectors.mqtt.MqttSubscriptions
import org.apache.pekko.stream.scaladsl.Sink
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence

import scala.collection.concurrent.TrieMap
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import io.circe.parser._

/**
 * Actor responsible for managing the state of a single sensor topic.
 * Each unique topic discovered by the wildcard subscription gets its own actor.
 * @param topic The specific MQTT topic this actor is handling.
 */
class TopicActor(topic: String) extends Actor {
  // Local state to keep track of cumulative sum and latest data point
  var sum = 0
  var lastTimestamp = ""

  override def receive: Receive = {
    case msg: MqttMessage =>
      // Parse the JSON payload using Circe
      parse(msg.payload.utf8String) match {
        case Right(json) =>
          val cursor = json.hcursor
          // Update local state from JSON fields
          cursor.get[Int]("value").foreach(v => sum += v)
          cursor.get[String]("timestamp").foreach(ts => lastTimestamp = ts)
          // Log current status for observability
          println(s"Topic: $topic, Sum: $sum, Last Timestamp: $lastTimestamp")
        case Left(err) =>
          // Log parsing errors
          System.err.println(s"Failed to parse JSON for topic $topic: ${err.getMessage}")
      }
    case other =>
      // Gracefully ignore unrecognized messages
      println(s"Unknown message received by TopicActor: $other")
  }
}

/**
 * Main entry point for the Subscriber service.
 * Subscribes to wildcard MQTT topics and dispatches messages to dedicated actors.
 */
object Main {
  def main(args: Array[String]): Unit = {
    // Initialize Pekko system and context
    implicit val system: ActorSystem = ActorSystem("subscriber")
    implicit val ec = system.dispatcher

    // Configure connection to the MQTT broker
    val connectionSettings = MqttConnectionSettings(
      "tcp://mosquitto:1883",
      "test-subscriber",
      new MemoryPersistence
    )

    // Define wildcard subscription: "sensors/#" matches all sub-topics under sensors
    val wildcardTopic = "sensors/#"
    val subscriptions = MqttSubscriptions(
      Map(wildcardTopic -> MqttQoS.AtLeastOnce)
    )

    // Create an MQTT source that emits messages arriving on the subscribed topics
    val mqttSource =
      MqttSource.atMostOnce(
        connectionSettings.withClientId("test-subscriber"),
        subscriptions,
        bufferSize = 8
      )

    // Thread-safe map to store and look up actors based on the MQTT topic name.
    // This ensures we have exactly one actor per sensor.
    val actors = TrieMap.empty[String, ActorRef]

    // Execute the stream processing pipeline:
    // 1. Source: Receives messages from Mosquitto
    // 2. Sink: Dispatches each message to the appropriate TopicActor
    mqttSource.runWith(Sink.foreach { msg =>
      val topic = msg.topic
      // Atomically get the existing actor or create a new one for this topic
      val actorRef = actors.getOrElseUpdate(
        topic,
        // Let Pekko manage the underlying actor creation and lifecycle
        system.actorOf(Props(new TopicActor(topic)))
      )
      // Send the message asynchronously to the actor
      actorRef ! msg
    })

    // Register a shutdown hook to ensure graceful termination of the ActorSystem
    sys.addShutdownHook {
      println("Shutting down subscriber...")
      system.terminate()
      Await.result(system.whenTerminated, 5.seconds)
    }
  }
}
