ThisBuild / scalaVersion := "3.8.2"

// Apache pekko settings
val PekkoVersion = "1.1.3"
lazy val pekkoSettings = Seq(
  libraryDependencies ++= Seq(
    "org.apache.pekko" %% "pekko-connectors-mqtt" % "1.1.0",
    "org.apache.pekko" %% "pekko-stream" % PekkoVersion
  )
)

// Service 1: Publisher
lazy val publisher = (project in file("service-publisher"))
  .settings(
    pekkoSettings,
    name := "publisher"
  )
