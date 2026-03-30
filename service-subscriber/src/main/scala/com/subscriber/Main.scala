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
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.time.OffsetDateTime
import io.circe.parser._

/** Database handler for TimescaleDB using HikariCP for connection pooling. */
object Database {
  private val config = new HikariConfig()
  config.setJdbcUrl(
    sys.env.getOrElse("DB_URL", "jdbc:postgresql://localhost:5432/epu")
  )
  config.setUsername(sys.env.getOrElse("DB_USER", "postgres"))
  config.setPassword(sys.env.getOrElse("DB_PASSWORD", "postgres"))
  config.addDataSourceProperty("cachePrepStmts", "true")
  config.addDataSourceProperty("prepStmtCacheSize", "250")
  config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048")

  val dataSource = new HikariDataSource(config)

  /** Inserts a sensor reading into the Data table.
    * @param deviceName
    *   The name of the device sending the data.
    * @param value
    *   The sensor reading value.
    * @param timestamp
    *   The ISO-8601 timestamp of the reading.
    */
  def insertData(deviceName: String, value: Int, timestamp: String): Unit = {
    val conn = dataSource.getConnection
    try {
      val stmt = conn.prepareStatement(
        "INSERT INTO Data (DeviceName, Value, Timestamp) VALUES (?, ?, ?)"
      )
      stmt.setString(1, deviceName)
      stmt.setInt(2, value)
      // Parse ISO timestamp to OffsetDateTime
      val ts = OffsetDateTime.parse(timestamp)
      stmt.setObject(3, ts)
      stmt.executeUpdate()
    } finally {
      conn.close()
    }
  }
}

/** Actor responsible for managing the state of a single sensor topic. Each
  * unique topic discovered by the wildcard subscription gets its own actor.
  * @param topic
  *   The specific MQTT topic this actor is handling.
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
          // Extract data and update local state
          val deviceNameOpt = cursor.get[String]("device_name").toOption
          val valueOpt = cursor.get[Int]("value").toOption
          val tsOpt = cursor.get[String]("timestamp").toOption

          (deviceNameOpt, valueOpt, tsOpt) match {
            case (Some(name), Some(v), Some(ts)) =>
              sum += v
              lastTimestamp = ts
              // Insert into TimescaleDB
              Database.insertData(name, v, ts)
              // Log current status for observability
              println(s"Topic: $topic, Sum: $sum, Last Timestamp: $lastTimestamp")
            case _ =>
              println(s"Missing fields in message for topic $topic")
          }
        case Left(err) =>
          // Log parsing errors
          System.err.println(
            s"Failed to parse JSON for topic $topic: ${err.getMessage}"
          )
      }
    case other =>
      // Gracefully ignore unrecognized messages
      println(s"Unknown message received by TopicActor: $other")
  }
}

/** Main entry point for the Subscriber service. Subscribes to wildcard MQTT
  * topics and dispatches messages to dedicated actors.
  */
object Main {
  def main(args: Array[String]): Unit = {
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
      Map(wildcardTopic -> MqttQoS.AtLeastOnce)
    )

    // Create an MQTT source that emits messages arriving on the subscribed topics
    val mqttSource =
      MqttSource.atMostOnce(
        mqttConnectionSettings.withClientId("test-subscriber"),
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
