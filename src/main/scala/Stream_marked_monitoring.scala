import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}


object Stream_marked_monitoring extends  App {

  val spark = SparkSession
    .builder()
    .appName("Streaming kafka handler")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  //val topicinput = "items_marked"
  val topicinput = "anomalies_marked"

  val itemsschema = new StructType()
    .add("Curts", LongType)
    .add("X", IntegerType)
    .add("Y", IntegerType)
    .add("Z", IntegerType)
    .add("annom-flag", IntegerType)

  import spark.implicits._
  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", topicinput)
    .option("startingOffsets", "latest")
    .load()

  val df1 = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .as[(String, String)]

  val df3 = df1.select(from_json(col("value"), itemsschema).as("data")).select("data.*")

  val df4 = df3.writeStream
    .format("console")
    .outputMode("append")
    .start()
    .awaitTermination()
}

