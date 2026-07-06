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

// Entry point: wires Pekko, MQTT stream, per-topic actor registry, heartbeat scheduler, and shutdown hook
object Main {
  // Initialise metrics, connect to MQTT, and run until the JVM exits
  def main(args: Array[String]): Unit = {
    Metrics.init(8081)

    implicit val system: ActorSystem = ActorSystem("subscriber")
    implicit val ec: ExecutionContext = system.dispatcher

    // system is implicit so Database.backend passes it to the backend constructor.
    val db = Database.backend

    val mqttConnectionSettings = MqttConnectionSettings(
      "tcp://mosquitto:1883",
      "test-subscriber",
      new MemoryPersistence
    )

    val wildcardTopic  = "sensors/#"
    val subscriptions  = MqttSubscriptions(Map(wildcardTopic -> MqttQoS.AtMostOnce))

    val mqttSource =
      MqttSource.atMostOnce(
        mqttConnectionSettings.withClientId("test-subscriber"),
        subscriptions,
        bufferSize = 1024
      )

    // How often every actor is pinged to re-evaluate its sensor's liveness. Coupled with
    // LivenessTimeoutSeconds (1s) in TopicActor, which sets how long a sensor may be silent
    // before this check flips it to MISSING — keep the two in sync when tuning sensitivity.
    val heartbeatInterval = 5.seconds

    val actors = TrieMap.empty[String, ActorRef]

    mqttSource.runWith(Sink.foreach { msg =>
      val topic    = msg.topic
      val actorRef = actors.getOrElseUpdate(
        topic,
        system.actorOf(Props(new TopicActor(topic, db)))
      )
      actorRef ! msg
    })

    system.scheduler.scheduleWithFixedDelay(heartbeatInterval, heartbeatInterval) { () =>
      actors.values.foreach(_ ! "heartbeat")
    }

    sys.addShutdownHook {
      Metrics.stop()
      db.close()
      system.terminate()
      Await.result(system.whenTerminated, 5.seconds)
    }
  }
}
