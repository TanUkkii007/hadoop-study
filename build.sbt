name := "hadoop-study"

scalaVersion := "2.11.8"

val hadoopVersion = "2.7.2"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common" % hadoopVersion,
  "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion
)
