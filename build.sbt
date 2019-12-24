val kafkaVersion = "2.4.0"
val slf4jVersion = "1.7.29"
val twitterClientVersion = "4.0.7"
val elasticClientVersion = "6.5.4"
val gsonVersion = "2.8.6"


lazy val commonSettings = Seq(
  version := "0.1",
  organization := "com.github.anton-antonenko",
  scalaVersion := "2.13.1"
)


lazy val root = (project in file("."))
  .aggregate(basics, `twitter-producer`, `elasticsearch-consumer`)
  .settings(
    commonSettings,
    name := "kafka-training"
  )


lazy val basics = (project in file("basics"))
    .settings(
      name := "basics",
      commonSettings,
      libraryDependencies ++= Seq(
        "org.apache.kafka" % "kafka-clients" % kafkaVersion,
        "org.slf4j" % "slf4j-simple" % slf4jVersion
      )
    )


lazy val `twitter-producer` = (project in file("twitter-producer"))
  .settings(
    name := "twitter-producer",
    commonSettings,
    libraryDependencies ++= Seq(
      "org.apache.kafka" % "kafka-clients" % kafkaVersion,
      "org.slf4j" % "slf4j-simple" % slf4jVersion,
      "org.twitter4j" % "twitter4j-core" % twitterClientVersion,
      "org.twitter4j" % "twitter4j-stream" % twitterClientVersion,
      "com.google.code.gson" % "gson" % gsonVersion
    )
  )


lazy val `elasticsearch-consumer` = (project in file("elasticsearch-consumer"))
  .settings(
    name := "elasticsearch-consumer",
    commonSettings,
    libraryDependencies ++= Seq(
      "org.apache.kafka" % "kafka-clients" % kafkaVersion,
      "org.slf4j" % "slf4j-simple" % slf4jVersion,
      "org.elasticsearch.client" % "elasticsearch-rest-high-level-client" % elasticClientVersion,
      "com.google.code.gson" % "gson" % gsonVersion
    )
  )
