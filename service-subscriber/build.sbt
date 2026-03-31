scalaVersion := "3.8.2"

val PekkoVersion = "1.1.3"
libraryDependencies ++= Seq(
  "org.apache.pekko" %% "pekko-connectors-mqtt" % "1.1.0",
  "org.apache.pekko" %% "pekko-stream" % PekkoVersion,
  "org.apache.pekko" %% "pekko-slf4j" % PekkoVersion,
  "org.eclipse.paho" % "org.eclipse.paho.client.mqttv3" % "1.2.5",
  "ch.qos.logback" % "logback-classic" % "1.5.6"
)

libraryDependencies ++= Seq(
  "io.circe" %% "circe-core" % "0.14.6",
  "io.circe" %% "circe-parser" % "0.14.6",
  "io.circe" %% "circe-generic" % "0.14.6",
  "org.postgresql" % "postgresql" % "42.7.2",
  "com.zaxxer" % "HikariCP" % "5.1.0",
  "io.prometheus" % "simpleclient" % "0.16.0",
  "io.prometheus" % "simpleclient_httpserver" % "0.16.0",
  "io.prometheus" % "simpleclient_hotspot" % "0.16.0"
)

assembly / assemblyMergeStrategy := {
  // Discard module-info.class files to avoid deduplication conflicts.
  // These files are used by JPMS (Java 9+), but are not needed when 
  // running on the classpath as a fat JAR.
  case PathList("module-info.class") => MergeStrategy.discard
  case x if x.endsWith("/module-info.class") => MergeStrategy.discard
  case x =>
    val oldStrategy = (assembly / assemblyMergeStrategy).value
    oldStrategy(x)
}
