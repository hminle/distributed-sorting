name := "distributed-sorting"

version := "1.0"

scalaVersion := "2.11.8"

//libraryDependencies += "org.scala-lang.modules" %% "scala-pickling" % "0.10.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0" % "test"

libraryDependencies ++= Seq(
  "junit" % "junit" % "4.8.1" % "test"
)
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.7"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0"
libraryDependencies += "com.lihaoyi" %% "upickle" % "0.4.3"