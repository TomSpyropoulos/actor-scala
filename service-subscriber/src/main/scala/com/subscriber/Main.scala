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
import scala.util.{Failure, Success}
import scala.util.control.NonFatal

// Entry point: wires Pekko, MQTT stream, per-topic actor registry, read load, heartbeat scheduler, and shutdown hook
object Main {
  // Initialise metrics, connect to MQTT, and run until the JVM exits
  def main(args: Array[String]): Unit = {
    Metrics.init(8081)

    implicit val system: ActorSystem = ActorSystem("subscriber")
    implicit val ec: ExecutionContext = system.dispatcher

    try {
      // system is implicit so Database.backend passes it to the backend constructor.
      val db = Database.backend

      // Artificial read load: an independent branch of the system, not part of the ingest path.
      // Nothing routes to it and it holds no per-topic state, so it starts before the MQTT stream and
      // runs whether or not any sensor is publishing. None when READS_PER_SEC is 0, which is what
      // makes "no reads" mean no actors and no connections rather than idle readers.
      val readers: Option[ReaderPool] =
        if (ReadConfig.enabled)
          Some(new ReaderPool(ReadConfig.periodMs, ReadConfig.readers, Database.readTarget))
        else None

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

      // runWith's Future is the only place a broker disconnect or a subscribe failure surfaces;
      // discarding it would leave a live JVM consuming nothing. A normal completion is just as
      // fatal here — this stream is meant to run for the process's whole lifetime.
      mqttSource.runWith(Sink.foreach { msg =>
        val topic    = msg.topic
        val actorRef = actors.getOrElseUpdate(
          topic,
          system.actorOf(Props(new TopicActor(topic, db)))
        )
        actorRef ! msg
      }).onComplete {
        case Failure(e) => fatal("MQTT stream failed", e)
        case Success(_) => fatal("MQTT stream ended", new IllegalStateException("MqttSource completed"))
      }

      system.scheduler.scheduleWithFixedDelay(heartbeatInterval, heartbeatInterval) { () =>
        actors.values.foreach(_ ! "heartbeat")
      }

      sys.addShutdownHook {
        Metrics.stop()
        readers.foreach(_.close())
        db.close()
        system.terminate()
        Await.result(system.whenTerminated, 5.seconds)
      }
    } catch {
      // Most often a refused JDBC connection: TimescaleDB accepts socket connections while it runs
      // init.sql, then restarts to bind TCP, and HikariCP's fail-fast pool init lands in that window.
      case NonFatal(e) => fatal("subscriber startup failed", e)
    }
  }

  // Kill the JVM rather than just the calling thread. Metrics' HTTP server and the actor system
  // both run on non-daemon threads, so an escaping exception would otherwise leave a process that
  // still answers /metrics with every counter at zero — indistinguishable from a healthy but idle
  // subscriber, and never restarted by Docker because the container's main process is still alive.
  private def fatal(context: String, e: Throwable): Nothing = {
    System.err.println(s"FATAL: $context: ${e.getMessage}")
    e.printStackTrace()
    sys.exit(1)
  }
}
