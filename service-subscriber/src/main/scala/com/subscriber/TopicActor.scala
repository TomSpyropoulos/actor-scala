package com.subscriber

import org.apache.pekko.actor.{Actor, ActorLogging}
import org.apache.pekko.stream.connectors.mqtt.MqttMessage
import io.circe.parser._
import java.time.OffsetDateTime

import scala.concurrent.ExecutionContext

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