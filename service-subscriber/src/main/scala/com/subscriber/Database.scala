package com.subscriber

import org.apache.pekko.actor.ActorSystem

// The one place the database connection is read from the environment, shared by the write backend
// and the read target so the two can never drift apart. Every backend reads these same five keys,
// and so does the Erlang arm -- see db_backend_timescaledb.erl and db_backend_mysql.erl -- which is
// what lets one docker-compose.<backend>.yaml configure both repos identically. The defaults are a
// bare fallback only; compose always supplies all five.
object DbConfig {
  val host: String     = sys.env.getOrElse("DB_HOST",     "timescaledb")
  val port: String     = sys.env.getOrElse("DB_PORT",     "5432")
  val name: String     = sys.env.getOrElse("DB_NAME",     "epu")
  val user: String     = sys.env.getOrElse("DB_USER",     "postgres")
  val password: String = sys.env.getOrElse("DB_PASSWORD", "postgres")

  // Assembled from the parts rather than read as a whole DB_URL: a single JDBC key on this side and
  // DB_HOST/DB_NAME on the Erlang side is what made the two arms' scenario files non-identical.
  //
  // The scheme is the caller's, not this object's: it is a property of the driver, so it belongs to
  // the backend that chose the driver. `params` carries whatever connection options that driver
  // needs in the URL -- Connector/J takes several, pgjdbc takes none.
  def urlFor(driver: String, params: String = ""): String =
    s"jdbc:$driver://$host:$port/$name$params"
}

// Reads DB_BACKEND env var and returns the matching write backend and read target; called from Main
object Database {
  private val Supported = "timescaledb, mysql"

  // Fails fast on an unrecognised backend name so misconfiguration is caught at startup
  def backend(implicit system: ActorSystem): DatabaseBackend =
    sys.env.getOrElse("DB_BACKEND", "timescaledb") match {
      case "timescaledb" => new TimescaleDBBackend()
      case "mysql"       => new MySQLBackend()
      case unknown =>
        throw new IllegalArgumentException(
          s"Unknown DB_BACKEND: '$unknown'. Supported: $Supported")
    }

  // A factory rather than an instance: every ReaderActor builds its own target so the connection is
  // opened on that actor's own dispatcher thread. Resolved from the same DB_BACKEND as the write
  // backend, and failing the same way, so a backend name is either supported for both or for neither.
  // Each arm builds its URL with the same urlFor call its write backend uses, so the read and write
  // paths cannot be pointed at different databases or opened with different connection options.
  def readTarget: () => ReadTarget =
    sys.env.getOrElse("DB_BACKEND", "timescaledb") match {
      case "timescaledb" =>
        () =>
          new TimescaleReadTarget(
            DbConfig.urlFor(TimescaleDBBackend.Driver), DbConfig.user, DbConfig.password)
      case "mysql" =>
        () =>
          new MySQLReadTarget(
            DbConfig.urlFor(MySQLBackend.Driver, MySQLBackend.UrlParams),
            DbConfig.user, DbConfig.password)
      case unknown =>
        throw new IllegalArgumentException(
          s"Unknown DB_BACKEND: '$unknown'. Supported: $Supported")
    }
}
