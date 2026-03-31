package com.subscriber

import org.apache.pekko.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import org.apache.pekko.stream.connectors.mqtt.MqttConnectionSettings
import org.apache.pekko.stream.connectors.mqtt.MqttMessage
import org.apache.pekko.stream.connectors.mqtt.MqttQoS
import org.apache.pekko.stream.connectors.mqtt.scaladsl.MqttSource
import org.apache.pekko.stream.connectors.mqtt.MqttSubscriptions
import org.apache.pekko.stream.scaladsl.Sink
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence

import scala.collection.concurrent.TrieMap
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.DurationInt
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.time.OffsetDateTime
import io.circe.parser._
import org.apache.pekko.pattern.pipe

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
  // Optimize connection pool size for blocking I/O dispatcher
  config.setMaximumPoolSize(20)

  val dataSource = new HikariDataSource(config)

  /** Inserts a sensor reading into the Data table asynchronously.
    * @param deviceName
    *   The name of the device sending the data.
    * @param value
    *   The sensor reading value.
    * @param timestamp
    *   The ISO-8601 timestamp of the reading.
    * @param ec
    *   Execution context (should be a dedicated dispatcher for blocking I/O).
    */
  def insertData(deviceName: String, value: Int, timestamp: String)(implicit ec: ExecutionContext): Future[Unit] = Future {
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
      ()
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
class TopicActor(topic: String) extends Actor with ActorLogging {
  // Use a dedicated dispatcher for blocking DB operations to avoid starvation
  implicit val blockingDispatcher: ExecutionContext = context.system.dispatchers.lookup("pekko.actor.blocking-io-dispatcher")

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
              // --- Prometheus Metrics Recording ---
              val startTime = OffsetDateTime.parse(ts)
              val now = OffsetDateTime.now()
              val latency =
                java.time.Duration.between(startTime, now).toMillis.toDouble

              Metrics.requestCount.inc()
              Metrics.requestLatency.observe(latency)
              // -------------------------------------

              sum += v
              lastTimestamp = ts
              
              // Asynchronous insert using dedicated dispatcher
              Database.insertData(name, v, ts).failed.foreach { err =>
                log.error(s"Failed to insert data for topic $topic: ${err.getMessage}")
              }
              
              // Log using ActorLogging (asynchronous)
              if (log.isDebugEnabled) {
                log.debug("Topic: {}, Sum: {}, Last Timestamp: {}, Latency: {}ms", topic, sum, lastTimestamp, latency)
              }
            case _ =>
              log.warning("Missing fields in message for topic {}", topic)
          }
        case Left(err) =>
          log.error("Failed to parse JSON for topic {}: {}", topic, err.getMessage)
      }
    case other =>
      log.warning("Unknown message received by TopicActor: {}", other)
  }
}

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
      Map(wildcardTopic -> MqttQoS.AtLeastOnce)
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

    // Register a shutdown hook
    sys.addShutdownHook {
      Metrics.stop()
      system.terminate()
      Await.result(system.whenTerminated, 5.seconds)
    }
  }
}
