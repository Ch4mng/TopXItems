import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.rdd.RDD

object Main {
  def main(args: Array[String]): Unit = {
    // Read command-line arguments
    val datasetAPath = args(0) // Parquet path for Dataset A
    val datasetBPath = args(1) // Parquet path for Dataset B
    val outputPath = args(2)   // Output Parquet file path
    val topX = args(3).toInt   // Number of top items

    // Initialize Spark session
    val spark = SparkSession.builder()
      .appName("TopXItems")
      .master("local[*]")  // Use local mode for testing
      .getOrCreate()

    // Read Parquet files as DataFrames
    val datasetA: DataFrame = spark.read.parquet(datasetAPath)
    val datasetB: DataFrame = spark.read.parquet(datasetBPath)

    // Convert DataFrames to RDDs
    val datasetARdd: RDD[(Long, Long, Long, String, Long)] = datasetA.rdd.map(row =>
      (row.getLong(0), row.getLong(1), row.getLong(2), row.getString(3), row.getLong(4))
    )

    val datasetBRdd: RDD[(Long, String)] = datasetB.rdd.map(row =>
      (row.getLong(0), row.getString(1))
    )

    // Process data using SparkApp
    val outputRDD = SparkApp.process(datasetARdd, datasetBRdd, topX)

    // Convert to DataFrame & Write Output
    import spark.implicits._
    val outputDF = outputRDD.map { case (geoLoc, rank, item) =>
      (geoLoc, rank, item)
    }.toDF("geographical_location", "item_rank", "item_name")

    outputDF.write.mode("overwrite").parquet(outputPath)

    println(s"✅ Processed data written to: $outputPath")
    spark.stop()
  }
}
