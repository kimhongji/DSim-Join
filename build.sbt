lazy val root = (project in file(".")).settings(
name := "Dima-DS",
version := "1.0",
scalaVersion := "2.11.0"
)
libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % "2.1.0" % "provided",
	"org.apache.spark" %% "spark-streaming" % "2.0.0" % "provided",
	"org.apache.spark" %% "spark-sql" % "2.2.2",
	"org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.1.0" % "provided",
	"org.mongodb.spark" %% "mongo-spark-connector" % "2.1.0",
	"org.mongodb.scala" %% "mongo-scala-driver" % "2.0.0",
	"org.mongodb.scala" %% "mongo-scala-bson" % "2.0.0",
	"org.mongodb" % "bson" % "3.4.2",
	"org.mongodb" % "mongodb-driver-core" % "3.4.2",
	"org.mongodb" % "mongodb-driver-async" % "3.4.2",
	//"org.mongodb" %% "casbah" % "2.8.0",
	"org.json4s" %% "json4s-native" % "3.2.11",
	"org.json4s" %% "json4s-jackson" % "3.2.11"
)

mergeStrategy in assembly := {
	case PathList("META-INF", xs @ _*) => MergeStrategy.discard
	case x => MergeStrategy.first
}

javaOptions in run ++= Seq(
	"-Dlog4j.debug=true",
	"-Dlog4j.configuration=log4j.properties"
)

