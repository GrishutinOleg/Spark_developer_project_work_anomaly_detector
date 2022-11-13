import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter


object batch_translator extends App {

  val spark = SparkSession
    .builder()
    .appName("Batch kafka translator")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val topicinput = "items_generated"

  import spark.implicits._
  val df = spark
    .read
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", topicinput)
    .option("checkpointLocation", "/tmp/spark-batch-pca-reading/checkpoint-translator")
    .load()

  val itemsschema = new StructType()
    .add("Curts", LongType)
    .add("X", IntegerType)
    .add("Y", IntegerType)
    .add("Z", IntegerType)

  val df1 = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .as[(String, String)]

  df1.show()

  df1.printSchema()

  val df3 = df1.select(from_json(col("value"), itemsschema).as("data")).select("data.*")

  df3.show(10)

  df3.printSchema()

  val prevdate = DateTimeFormatter.ofPattern("yyyy-MM-dd").format(LocalDateTime.now.minusDays(1))
  val mypathrawdata = "src/main/resources/data/raw_data-" + prevdate

  df3.write.mode("overwrite").json(mypathrawdata)

}
