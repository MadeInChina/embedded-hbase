name := "embedded-hbase"

version := "1.0"


scalaVersion := "2.11.7"

libraryDependencies ++= library.hbase

parallelExecution in ThisBuild := false