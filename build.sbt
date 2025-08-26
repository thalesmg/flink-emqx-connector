val scala3Version = "3.7.2"

lazy val root = project
  .in(file("."))
  .settings(
    name := "Flink EMQX Connector",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala3Version,

    libraryDependencies += "org.scalameta" %% "munit" % "1.0.0" % Test,
    // https://mvnrepository.com/artifact/org.apache.flink/flink-core
    libraryDependencies += "org.apache.flink" % "flink-core" % "2.1.0" % "provided",
    // https://mvnrepository.com/artifact/org.apache.flink/flink-table-common
    libraryDependencies += "org.apache.flink" % "flink-table-common" % "2.1.0" % "provided",
    // https://mvnrepository.com/artifact/org.eclipse.paho/org.eclipse.paho.mqttv5.client
    libraryDependencies += "org.eclipse.paho" % "org.eclipse.paho.mqttv5.client" % "1.2.5",

    Test / fork := true,

    libraryDependencies += "com.dimafeng" %% "testcontainers-scala-munit" % "0.43.0" % Test
  )
