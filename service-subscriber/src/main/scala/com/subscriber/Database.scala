package com.subscriber

import org.apache.pekko.actor.ActorSystem

// The one place the database connection is read from the environment, shared by the write backend
// and the read target so the two can never drift apart. The Erlang arm reads the same five keys in
// db_backend_timescaledb.erl and db_read_backend_timescaledb.erl, and docker-compose.timescaledb.yaml
// supplies them identically in both repos -- keep all three in sync.
object DbConfig {
  val host: String     = sys.env.getOrElse("DB_HOST",     "timescaledb")
  val port: String     = sys.env.getOrElse("DB_PORT",     "5432")
  val name: String     = sys.env.getOrElse("DB_NAME",     "epu")
  val user: String     = sys.env.getOrElse("DB_USER",     "postgres")
  val password: String = sys.env.getOrElse("DB_PASSWORD", "postgres")

  // Assembled from the parts rather than read as a whole DB_URL: a single JDBC key on this side and
  // DB_HOST/DB_NAME on the Erlang side is what made the two arms' scenario files non-identical.
  val url: String = s"jdbc:postgresql://$host:$port/$name"
}

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
  // Connection details come from DbConfig, the same object TimescaleDBBackend reads, so the read and
  // write paths cannot be pointed at different databases.
  def readTarget: () => ReadTarget =
    sys.env.getOrElse("DB_BACKEND", "timescaledb") match {
      case "timescaledb" =>
        () =>
          new TimescaleReadTarget(DbConfig.url, DbConfig.user, DbConfig.password)
      case unknown =>
        throw new IllegalArgumentException(
          s"Unknown DB_BACKEND: '$unknown'. Supported: timescaledb")
    }
}
