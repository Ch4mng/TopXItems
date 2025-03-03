import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.scalatest.funsuite.AnyFunSuite

class SparkAppTest extends AnyFunSuite {

  lazy val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("SparkUnitTest")
    .getOrCreate()

  test("Test Top X Items Computation with Deduplication and Ranking") {
    val datasetA: RDD[(Long, Long, Long, String, Long)] = spark.sparkContext.parallelize(Seq(
      (1L, 101L, 1001L, "item1", 1633024800L),
      (1L, 102L, 1002L, "item2", 1633024800L),
      (1L, 101L, 1001L, "item1", 1633024800L), // Duplicate detection_oid
      (1L, 103L, 1003L, "item3", 1633024800L),
      (1L, 104L, 1004L, "item1", 1633024800L),
      (1L, 105L, 1005L, "item2", 1633024800L)
    ))

    val datasetB: RDD[(Long, String)] = spark.sparkContext.parallelize(Seq(
      (1L, "Location1")
    ))

    val result: Array[(String, Int, String)] = SparkApp.process(datasetA, datasetB, topX = 2).collect()

    assert(result.length == 2)
    assert(result.contains(("Location1", 1, "item1")))
    assert(result.contains(("Location1", 2, "item2")))
  }

  test("Test Top X Items Computation with No Duplicates") {
    val datasetA: RDD[(Long, Long, Long, String, Long)] = spark.sparkContext.parallelize(Seq(
      (2L, 201L, 2001L, "itemA", 1633024800L),
      (2L, 202L, 2002L, "itemB", 1633024800L),
      (2L, 203L, 2003L, "itemC", 1633024800L)
    ))

    val datasetB: RDD[(Long, String)] = spark.sparkContext.parallelize(Seq(
      (2L, "Location2")
    ))

    val result: Array[(String, Int, String)] = SparkApp.process(datasetA, datasetB, topX = 2).collect()

    assert(result.length == 2)
    assert(result.contains(("Location2", 1, "itemA")))
    assert(result.contains(("Location2", 2, "itemB")))
  }

  test("Test Top X Items Computation with Multiple Locations") {
    val datasetA: RDD[(Long, Long, Long, String, Long)] = spark.sparkContext.parallelize(Seq(
      (1L, 101L, 1001L, "item1", 1633024800L),
      (1L, 102L, 1002L, "item2", 1633024800L),
      (2L, 201L, 2001L, "itemA", 1633024800L),
      (2L, 202L, 2002L, "itemB", 1633024800L)
    ))

    val datasetB: RDD[(Long, String)] = spark.sparkContext.parallelize(Seq(
      (1L, "Location1"),
      (2L, "Location2")
    ))

    val result: Array[(String, Int, String)] = SparkApp.process(datasetA, datasetB, topX = 2).collect()

    assert(result.length == 4)
    assert(result.contains(("Location1", 1, "item1")))
    assert(result.contains(("Location1", 2, "item2")))
    assert(result.contains(("Location2", 1, "itemA")))
    assert(result.contains(("Location2", 2, "itemB")))
  }
}
