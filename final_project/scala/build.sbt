name := "bigdata_project"
version := "1.0"
scalaVersion := "2.11.6"

assemblyJarName in assembly := "app.jar"
test in assembly := {}

val sparkVersion = "2.3.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.jwat" % "jwat-warc" % "1.1.1",
  "org.apache.pig" % "pig" % "0.17.0" % "provided"
)
