package com.subscriber

/** Selects and instantiates the active database backend at JVM startup.
  *
  * The backend is chosen based on the DB_BACKEND environment variable
  * (default: "timescaledb"). The single instance is created here and passed
  * to every TopicActor via constructor injection in [[Main]], so the backend
  * can be swapped without touching actor code.
  *
  * To add a new backend: implement [[DatabaseBackend]] and add a case below.
  */
object Database {
  val backend: DatabaseBackend =
    sys.env.getOrElse("DB_BACKEND", "timescaledb") match {
      case "timescaledb" => new TimescaleDBBackend()
      case unknown =>
        throw new IllegalArgumentException(
          s"Unknown DB_BACKEND: '$unknown'. Supported: timescaledb")
    }
}
