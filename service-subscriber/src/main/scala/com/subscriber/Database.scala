package com.subscriber

import org.apache.pekko.actor.ActorSystem

// Reads DB_BACKEND env var and returns the matching backend instance; single call site in Main
object Database {
  // Fails fast on an unrecognised backend name so misconfiguration is caught at startup
  def backend(implicit system: ActorSystem): DatabaseBackend =
    sys.env.getOrElse("DB_BACKEND", "timescaledb") match {
      case "timescaledb" => new TimescaleDBBackend()
      case unknown =>
        throw new IllegalArgumentException(
          s"Unknown DB_BACKEND: '$unknown'. Supported: timescaledb")
    }
}
