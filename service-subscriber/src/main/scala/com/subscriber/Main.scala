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

class TopicActor(topic: String) extends Actor {
  var sum = 0
  var lastTimestamp = ""
  override def receive: Receive = {
    case msg: MqttMessage =>
      parse(msg.payload.utf8String) match {
        case Right(json) =>
          val cursor = json.hcursor
          cursor.get[Int]("value").foreach(v => sum += v)
          cursor.get[String]("timestamp").foreach(ts => lastTimestamp = ts)
          println(s"Topic: $topic, Sum: $sum, Last Timestamp: $lastTimestamp")
        case Left(err) =>
        // handle parse error
      }
    case other =>
    // ignore / handle other messages
  }
}

object Main {
  def main(args: Array[String]): Unit = {
    implicit val system: ActorSystem = ActorSystem("subscriber")
    implicit val ec = system.dispatcher

    // Create connection settings
    val connectionSettings = MqttConnectionSettings(
      "tcp://mosquitto:1883",
      "test-subscriber",
      new MemoryPersistence
    )

    //  Wildcard topic subscription
    val wildcardTopic = "sensors/#"
    val subscriptions = MqttSubscriptions(
      Map(wildcardTopic -> MqttQoS.AtLeastOnce)
    )

    val mqttSource =
      MqttSource.atMostOnce(
        connectionSettings.withClientId("test-subscriber"),
        subscriptions,
        bufferSize = 8
      )

    // cache actors per topic (thread-safe)
    val actors = TrieMap.empty[String, ActorRef]

    // run the stream and dispatch each incoming message to a per-topic actor
    mqttSource.runWith(Sink.foreach { msg =>
      // MqttMessage has a .topic and .payload (ByteString)
      val topic = msg.topic
      val actorRef = actors.getOrElseUpdate(
        topic,
        // do not provide a name (let Pekko generate a valid actor name)
        system.actorOf(Props(new TopicActor(topic)))
      )
      actorRef ! msg
    })

    // keep the JVM alive until termination (Ctrl-C or external termination)
    sys.addShutdownHook {
      system.terminate()
      Await.result(system.whenTerminated, 5.seconds)
    }
  }
}
