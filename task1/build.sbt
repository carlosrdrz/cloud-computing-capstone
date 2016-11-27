name := "capstone"

version := "1.0"

scalaVersion := "2.12.0"

assemblyJarName in assembly := "capstone.jar"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common" % "2.7.3" % "provided",
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "2.7.3" % "provided",
  "org.apache.hadoop" % "hadoop-client" % "2.7.3" % "provided"
)
