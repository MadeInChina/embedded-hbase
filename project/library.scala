import sbt._


object library {
  val hbase = Seq(
    "org.apache.hbase" % "hbase" % "0.94.15-cdh4.7.1"
      withSources(),
    "org.apache.hadoop" % "hadoop-common" % "2.0.0-cdh4.7.1"
      withSources()
  )
}