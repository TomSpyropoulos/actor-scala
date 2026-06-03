package com.subscriber

import org.apache.pekko.actor.{Actor, ActorLogging}
import org.apache.pekko.stream.connectors.mqtt.MqttMessage
import io.circe.parser._
import java.time.OffsetDateTime

import scala.concurrent.ExecutionContext

/** Actor responsible for managing the state of a single sensor topic. Each
  * unique topic discovered by the wildcard subscription gets its own actor.
  * @param topic The MQTT topic this actor handles (e.g. "sensors/device123").
  * @param db    Database backend used for all writes; injected at construction
  *              so the backend can be swapped at startup without modifying this actor.
  */
class TopicActor(topic: String, db: DatabaseBackend) extends Actor with ActorLogging {
  // Use a dedicated dispatcher for blocking DB operations to avoid starvation
  implicit val blockingDispatcher: ExecutionContext = context.system.dispatchers.lookup("pekko.actor.blocking-io-dispatcher")

  // Local state to keep track of cumulative sum and last seen timestamp (epoch seconds)
  var sum = 0
  var lastSeen: Long = 0L
  var lastStatus: Option[String] = None

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
              lastSeen = java.time.Instant.now().getEpochSecond

              val publisherEpochMs = startTime.toInstant.toEpochMilli

              val subscriberStartMs = now.toInstant.toEpochMilli

              // Asynchronous insert using dedicated dispatcher
              db.insertData(name, v, ts).andThen { case _ =>
                val dbAckMs = System.currentTimeMillis()
                Metrics.e2eLatency.observe(math.max(0, dbAckMs - publisherEpochMs))
                Metrics.dbWriteLatency.observe(math.max(0, dbAckMs - subscriberStartMs))
              }.failed.foreach { err =>
                log.error(s"Failed to insert data for topic $topic: ${err.getMessage}")
              }
            case _ =>
              log.warning("Missing fields in message for topic {}", topic)
          }
        case Left(err) =>
          log.error("Failed to parse JSON for topic {}: {}", topic, err.getMessage)
      }
    case "heartbeat" =>
      val now = java.time.Instant.now().getEpochSecond

      val (newStatus, shouldInsert) = if (lastSeen == 0L) {
        ("MISSING", lastStatus != Some("MISSING"))
      } else {
        val diff = now - lastSeen
        val status = if (diff > 1) "MISSING" else "ALIVE"
        (status, lastStatus != Some(status))
      }

      val deviceName = topic.stripPrefix("sensors/")

      if (shouldInsert) {
        log.warning("Sensor {} is now {}", topic, newStatus)
        db.insertStatus(deviceName, newStatus).failed.foreach { err =>
          log.error(s"Failed to insert status for $topic: ${err.getMessage}")
        }
        lastStatus = Some(newStatus)
      }

      // Always update the gauge so Prometheus always reflects the current state,
      // even when the status hasn't changed since the last heartbeat.
      Metrics.sensorUp.labels(deviceName).set(if (newStatus == "ALIVE") 1 else 0)
    case other =>
      log.warning("Unknown message received by TopicActor: {}", other)
  }
}