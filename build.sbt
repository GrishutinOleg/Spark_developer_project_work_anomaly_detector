ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "Spark_developer_project_work_anomaly_detector"
  )

val sparkVersion = "3.3.0"
val kafkaVersion = "3.2.1"
val circeVersion = "0.14.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion
  , "org.apache.spark" %% "spark-mllib" % sparkVersion
  , "org.apache.spark" %% "spark-core" % sparkVersion
  , "org.apache.kafka" % "kafka-clients" % kafkaVersion
  , "org.codehaus.jackson" % "jackson-core-asl" % "1.9.13"
  , "io.circe" %% "circe-core" % circeVersion
  , "io.circe" %% "circe-generic" % circeVersion
  , "io.circe" %% "circe-parser" % circeVersion
  , "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.3.0"

)

