scalaVersion := "3.8.2"

val PekkoVersion = "1.1.3"
libraryDependencies ++= Seq(
  "org.apache.pekko" %% "pekko-connectors-mqtt" % "1.1.0",
  "org.apache.pekko" %% "pekko-stream" % PekkoVersion,
  "org.eclipse.paho" % "org.eclipse.paho.client.mqttv3" % "1.2.5"
)

libraryDependencies ++= Seq(
  "io.circe" %% "circe-core" % "0.14.6",
  "io.circe" %% "circe-parser" % "0.14.6",
  "io.circe" %% "circe-generic" % "0.14.6",
  "org.postgresql" % "postgresql" % "42.7.2",
  "com.zaxxer" % "HikariCP" % "5.1.0"
)
