val scala3Version = "3.7.2"

lazy val root = project
  .in(file("."))
  .settings(
    name := "Flink EMQX Connector",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala3Version,

    // https://mvnrepository.com/artifact/org.apache.flink/flink-core
    libraryDependencies += "org.apache.flink" % "flink-core" % "2.1.0" % "provided",
    // https://mvnrepository.com/artifact/org.apache.flink/flink-table-common
    libraryDependencies += "org.apache.flink" % "flink-table-common" % "2.1.0" % "provided",
    libraryDependencies += "org.apache.flink" % "flink-table-runtime" % "2.1.0" % "provided",
    libraryDependencies += "org.apache.flink" % "flink-connector-base" % "2.1.0" % "provided",
    // https://mvnrepository.com/artifact/org.eclipse.paho/org.eclipse.paho.mqttv5.client
    libraryDependencies += "org.eclipse.paho" % "org.eclipse.paho.mqttv5.client" % "1.2.5",

    // https://mvnrepository.com/artifact/org.apache.flink/flink-streaming-java
    // libraryDependencies += "org.apache.flink" % "flink-runtime" % "2.1.0" % "runtime, provided",
    // libraryDependencies += "org.apache.flink" % "flink-clients" % "2.1.0" % "runtime, provided",

    Test / fork := true,

    libraryDependencies += "org.scalameta" %% "munit" % "1.0.0" % Test,
    libraryDependencies += "com.dimafeng" %% "testcontainers-scala-munit" % "0.43.0" % Test,
    libraryDependencies += "org.apache.flink" % "flink-test-utils" % "2.1.0" % Test,
    // libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.19.2" % Test,
  )

lazy val poc = project
  .in(file("poc"))
  .dependsOn(root % "compile->compile;compile->test")
  .settings(
    scalaVersion := scala3Version,

    publish / skip := true,

    // vital to avoid bizarre `ClassNotFoundException`s ...
    fork := true,

    libraryDependencies += "org.apache.flink" % "flink-core" % "2.1.0",
    libraryDependencies += "org.apache.flink" % "flink-runtime" % "2.1.0",
    libraryDependencies += "org.apache.flink" % "flink-clients" % "2.1.0",

    // libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.10",
    // libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4"
  )
