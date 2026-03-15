scalaVersion := "3.8.2"

val PekkoVersion = "1.1.3"
libraryDependencies ++= Seq(
  "org.apache.pekko" %% "pekko-connectors-mqtt" % "1.1.0",
  "org.apache.pekko" %% "pekko-stream" % PekkoVersion,
  "org.eclipse.paho" % "org.eclipse.paho.client.mqttv3" % "1.2.5"
)
