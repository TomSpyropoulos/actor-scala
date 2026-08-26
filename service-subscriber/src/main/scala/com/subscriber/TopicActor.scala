package com.subscriber

import org.apache.pekko.actor.{Actor, ActorLogging}
import org.apache.pekko.stream.connectors.mqtt.MqttMessage
import io.circe.parser._
import java.time.{Instant, OffsetDateTime}

import scala.concurrent.ExecutionContext

// Per-sensor actor: decodes JSON payloads, records subscriber-receive latency, and delegates DB writes to the backend
class TopicActor(topic: String, db: DatabaseBackend) extends Actor with ActorLogging {
  // Use a dedicated dispatcher for blocking DB operations to avoid starvation
  implicit val blockingDispatcher: ExecutionContext = context.system.dispatchers.lookup("pekko.actor.blocking-io-dispatcher")

  // A sensor silent for longer than this at heartbeat time is declared MISSING. Coupled with the
  // heartbeat cadence in Main (scheduleWithFixedDelay, 5s) that drives how often this check runs.
  private val LivenessTimeoutSeconds = 1L

  var sum = 0
  var lastSeen: Long = 0L
  var lastStatus: Option[String] = None

  // Handles incoming MQTT payloads and periodic heartbeat ticks
  override def receive: Receive = {
    case msg: MqttMessage =>
      parse(msg.payload.utf8String) match {
        case Right(json) =>
          val cursor = json.hcursor
          val deviceNameOpt = cursor.get[String]("device_name").toOption
          val valueOpt      = cursor.get[Int]("value").toOption
          val tsOpt         = cursor.get[String]("timestamp").toOption

          (deviceNameOpt, valueOpt, tsOpt) match {
            case (Some(name), Some(v), Some(ts)) =>
              val publisherEpochUs    = Clock.micros(OffsetDateTime.parse(ts).toInstant)
              // Stamped after the parse, matching ProcessingStartUs in the Erlang worker so db_write
              // latency spans the same work in both arms
              val receivedAt          = Instant.now()
              val subscriberReceiveUs = Clock.micros(receivedAt)
              val latency             = math.max(0L, subscriberReceiveUs - publisherEpochUs) / 1000.0

              Metrics.requestCount.inc()
              Metrics.requestLatency.observe(latency)

              sum     += v
              // Reuses receivedAt; the drift is nothing against a 1s liveness timeout
              lastSeen = receivedAt.getEpochSecond

              // Backend owns e2e and db_write latency recording.
              db.insertData(name, v, publisherEpochUs, subscriberReceiveUs)
                .failed.foreach { err =>
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
        val diff   = now - lastSeen
        val status = if (diff > LivenessTimeoutSeconds) "MISSING" else "ALIVE"
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

      Metrics.sensorUp.labels(deviceName).set(if (newStatus == "ALIVE") 1 else 0)

    case other =>
      log.warning("Unknown message received by TopicActor: {}", other)
  }
}
