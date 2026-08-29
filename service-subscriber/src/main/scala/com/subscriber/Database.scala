package com.subscriber

import org.apache.pekko.actor.ActorSystem

// Reads DB_BACKEND env var and returns the matching write backend and read target; called from Main
object Database {
  // Fails fast on an unrecognised backend name so misconfiguration is caught at startup
  def backend(implicit system: ActorSystem): DatabaseBackend =
    sys.env.getOrElse("DB_BACKEND", "timescaledb") match {
      case "timescaledb" => new TimescaleDBBackend()
      case unknown =>
        throw new IllegalArgumentException(
          s"Unknown DB_BACKEND: '$unknown'. Supported: timescaledb")
    }

  // A factory rather than an instance: every ReaderActor builds its own target so the connection is
  // opened on that actor's own dispatcher thread. Resolved from the same DB_BACKEND as the write
  // backend, and failing the same way, so a backend name is either supported for both or for neither.
  // The connection defaults here must stay identical to TimescaleDBBackend's -- the Erlang arm
  // likewise has its read and write backends read the same DB_HOST/DB_USER/DB_PASSWORD/DB_NAME.
  def readTarget: () => ReadTarget =
    sys.env.getOrElse("DB_BACKEND", "timescaledb") match {
      case "timescaledb" =>
        () =>
          new TimescaleReadTarget(
            sys.env.getOrElse("DB_URL",      "jdbc:postgresql://timescaledb:5432/epu"),
            sys.env.getOrElse("DB_USER",     "postgres"),
            sys.env.getOrElse("DB_PASSWORD", "postgres"))
      case unknown =>
        throw new IllegalArgumentException(
          s"Unknown DB_BACKEND: '$unknown'. Supported: timescaledb")
    }
}
