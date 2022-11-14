import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import breeze.linalg.{DenseVector, inv}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{PCA, StandardScaler, VectorAssembler}
import org.apache.spark.ml.linalg.{Matrix, Vector}
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.{DataFrame, Row, SparkSession}



object Stream_handler extends App {

  val spark = SparkSession
    .builder()
    .appName("Streaming kafka handler")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val topicinput = "items_translated"
  val topicoutputall = "items_marked"
  val topicoutputabnom = "anomalies_marked"

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
    .option("failOnDataLoss","false")
    .load()


  val df1 = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .as[(String, String)]

  val df3 = df1.select(from_json(col("value"), itemsschema).as("data"))
    .select("data.*")

  def pcacalculation = (batchDF: DataFrame, batchId: Long) => {

    val recnumber = batchDF.count()
    println(s"batch id is ${batchId}, batch has ${recnumber} records")

    if (recnumber < 5) {

      val calculateddata = batchDF.select(col("Curts"), col("X"), col("Y"), col("Z"), lit(1).as("annom-flag"))

      println("All data")
      calculateddata.show()

      val annomdata = calculateddata.where(calculateddata("annom-flag") === 1)

      println("Abnormal data")
      annomdata.show()
      println("All data in this batch marked as abnormal")

      calculateddata
        .selectExpr("to_json(struct(*)) AS value")
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:29092")
        .option("topic", topicoutputall)
        .save()

      annomdata
        .selectExpr("to_json(struct(*)) AS value")
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:29092")
        .option("topic", topicoutputabnom)
        .save()

      println("Data saved in Kafka")
    }
    else {

      val vectorAssembler = new VectorAssembler()
        .setInputCols(Array("X", "Y", "Z"))
        .setOutputCol("vector")

      val standardScalar = new StandardScaler()
        .setInputCol("vector")
        .setOutputCol("normalized-vector")
        .setWithMean(true)
        .setWithStd(true)

      val pca = new PCA().setInputCol("normalized-vector").setOutputCol("pca-features").setK(2)

      val pipeline = new Pipeline().setStages(
        Array(vectorAssembler, standardScalar, pca)
      )

      val pcaDF = pipeline.fit(batchDF).transform(batchDF)

      def withMahalanobois(df: DataFrame, inputCol: String): DataFrame = {
        val Row(coeff1: Matrix) = Correlation.corr(df, inputCol).head

        val invCovariance = inv(new breeze.linalg.DenseMatrix(2, 2, coeff1.toArray))

        val mahalanobois = udf[Double, Vector] { v =>
          val vB = DenseVector(v.toArray)
          vB.t * invCovariance * vB
        }

        df.withColumn("mahalanobois", mahalanobois(df(inputCol)))
      }

      val withMahalanoboisDF: DataFrame = withMahalanobois(pcaDF, "pca-features")

      val withmarked = withMahalanoboisDF.withColumn("annom-flag", when(col("mahalanobois") > 3, 1).otherwise(0))

      val calculateddata = withmarked.select("Curts", "X", "Y", "Z", "annom-flag")

      println("All data")
      calculateddata.show()

      val annomdata = calculateddata.where(calculateddata("annom-flag") === 1)

      println("Abnormal data")
      annomdata.show()

      calculateddata
        .selectExpr("to_json(struct(*)) AS value")
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:29092")
        .option("topic", topicoutputall)
        .save()

      annomdata
        .selectExpr("to_json(struct(*)) AS value")
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:29092")
        .option("topic", topicoutputabnom)
        .save()

      println("Data saved in Kafka")
    }
  }

  val df4 = df3
    .writeStream
    .outputMode("append")
    .trigger(Trigger.ProcessingTime("1 minute"))
    .foreachBatch(pcacalculation)
    .option("checkpointLocation", "/tmp/spark-streaming-pca-writing/checkpoint-handler")
    .start()
    .awaitTermination()

}
