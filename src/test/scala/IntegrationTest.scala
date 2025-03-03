import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.DataFrame
import java.nio.file.Files

class IntegrationTest extends AnyFunSuite {

  lazy val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("SparkIntegrationTest")
    .getOrCreate()

  test("Integration Test - Read from Parquet, Process, and Write to Parquet") {
    // Create temporary directories for Parquet files
    val tempDir = Files.createTempDirectory("spark_test").toAbsolutePath.toString
    val datasetAPath = s"$tempDir/datasetA.parquet"
    val datasetBPath = s"$tempDir/datasetB.parquet"
    val outputPath = s"$tempDir/output.parquet"

    // Create test data for Dataset A
    val datasetA: DataFrame = spark.createDataFrame(Seq(
      (1L, 101L, 1001L, "item1", 1633024800L),
      (1L, 102L, 1002L, "item2", 1633024800L),
      (1L, 103L, 1003L, "item3", 1633024800L),
      (2L, 201L, 2001L, "itemA", 1633024800L),
      (2L, 202L, 2002L, "itemB", 1633024800L)
    )).toDF("geographical_location_oid", "video_camera_oid", "detection_oid", "item_name", "timestamp_detected")

    // Create test data for Dataset B
    val datasetB: DataFrame = spark.createDataFrame(Seq(
      (1L, "Location1"),
      (2L, "Location2")
    )).toDF("geographical_location_oid", "geographical_location")

    // Write test data to Parquet files
    datasetA.write.mode("overwrite").parquet(datasetAPath)
    datasetB.write.mode("overwrite").parquet(datasetBPath)

    // Run the Spark application using actual Parquet files
    Main.main(Array(datasetAPath, datasetBPath, outputPath, "2"))

    // Read the output and verify results
    val output: DataFrame = spark.read.parquet(outputPath)
    val results = output.collect().map(row => (row.getString(0), row.getInt(1), row.getString(2)))

    assert(results.length == 4)
    assert(results.contains(("Location1", 1, "item1")))
    assert(results.contains(("Location1", 2, "item2")))
    assert(results.contains(("Location2", 1, "itemA")))
    assert(results.contains(("Location2", 2, "itemB")))
  }
}

