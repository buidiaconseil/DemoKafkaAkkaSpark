// The simplest possible sbt build file is just one line:

scalaVersion := "2.12.6"

name := "AkkaStreamDemo"
organization := "com.buissondiaz"
version := "1.0"

libraryDependencies += "org.typelevel" %% "cats-core" % "1.1.0"
libraryDependencies += "com.typesafe.akka" %% "akka-stream" % "2.5.13"
libraryDependencies += "com.typesafe.akka" %% "akka-stream-kafka" % "0.22"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.4"
libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.8.4"
