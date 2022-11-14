import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}


object Stream_translator extends App {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("Streaming kafka translator")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val topicinput = "items_generated"

  val itemsschema = new StructType()
    .add("Curts", LongType)
    .add("X", IntegerType)
    .add("Y", IntegerType)
    .add("Z", IntegerType)

  import spark.implicits._

  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("subscribe", topicinput)
    .option("startingOffsets", "latest")
    .load()

  val df1 = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .as[(String, String)]

  df1.printSchema()

  val topicoutput = "items_translated"

  df1
    .writeStream
    .format("kafka")
    .outputMode("append")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("checkpointLocation", "/tmp/spark-streaming-pca-writing/checkpoint-translator")
    .option("topic", topicoutput)
    .start()
    .awaitTermination()

}
