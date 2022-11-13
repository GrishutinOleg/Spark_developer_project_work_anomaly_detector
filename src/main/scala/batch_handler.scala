import breeze.linalg.{DenseVector, inv}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{PCA, StandardScaler, VectorAssembler}
import org.apache.spark.ml.linalg.{Matrix, Vector}
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter


object batch_handler extends App {

  val spark = SparkSession.builder()
    .appName("batch_handler").master("local[*]").getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val prevdate = DateTimeFormatter.ofPattern("yyyy-MM-dd").format(LocalDateTime.now.minusDays(1))

  val mypathrawdata = "src/main/resources/data/raw_data-" + prevdate

  val rawDF = spark.read.format("json").load(mypathrawdata)

  val vectorAssembler = new VectorAssembler().setInputCols(Array("X", "Y", "Z")).setOutputCol("vector")

  val standardScalar = new StandardScaler()
    .setInputCol("vector")
    .setOutputCol("normalized-vector")
    .setWithMean(true)
    .setWithStd(true)

  val pca = new PCA().setInputCol("normalized-vector").setOutputCol("pca-features").setK(2)

  val pipeline = new Pipeline().setStages(
    Array(vectorAssembler, standardScalar, pca)
  )

  val pcaDF = pipeline.fit(rawDF).transform(rawDF)

  def withMahalanobois(df: DataFrame, inputCol: String): DataFrame = {
    val Row(coeff1: Matrix) = Correlation.corr(df, inputCol).head

    val invCovariance = inv(new breeze.linalg.DenseMatrix(2, 2, coeff1.toArray))

    val mahalanobois = udf[Double, Vector] { v =>
      val vB = DenseVector(v.toArray)
      vB.t * invCovariance * vB
    }

    df.withColumn("mahalanobois", mahalanobois(df(inputCol)))
  }

  val withMahalanobois: DataFrame = withMahalanobois(pcaDF, "pca-features")

  val withmarked = withMahalanobois.withColumn("annom-flag", when(col("mahalanobois") > 3, 1).otherwise(0))

  withmarked.cache()

  withmarked.show(200, truncate = false)

  val calculateddata = withmarked.select("Curts", "X", "Y", "Z", "annom-flag")

  val annomdata = calculateddata.where(calculateddata("annom-flag") === 1)

  annomdata.show(truncate = false)

  val mypathalldata = "src/main/resources/data/prediction_data-" + prevdate

  val mypathannomalis = "src/main/resources/data/annomal_data-" + prevdate

  calculateddata.write.mode("overwrite").json(mypathalldata)

  annomdata.write.mode("overwrite").json(mypathannomalis)

  spark.close()

}
