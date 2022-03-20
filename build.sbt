name := "kafka_client"

version := "0.1"

scalaVersion := "2.13.4"

val circeVersion = "0.12.0"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "2.6.0",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.0",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "org.apache.commons" % "commons-csv" % "1.8",
  "io.circe" %% "circe-core" % circeVersion,
  "io.circe" %% "circe-generic" % circeVersion
)