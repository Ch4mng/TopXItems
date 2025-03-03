import org.apache.spark.rdd.RDD

object SparkApp {
  def process(
    datasetA: RDD[(Long, Long, Long, String, Long)], // (geo_oid, camera_oid, detection_oid, item_name, timestamp)
    datasetB: RDD[(Long, String)], // (geo_oid, geographical_location)
    topX: Int
  ): RDD[(String, Int, String)] = { // (geographical_location, item_rank, item_name)

    // Deduplicate Dataset A based on detection_oid
    val dedupDatasetA = datasetA.map { case (geo, cam, detection, item, ts) =>
      (detection, (geo, cam, item, ts))
    }.reduceByKey((a, _) => a) // Keep only one instance per detection_oid
      .map(_._2) // Extract (geo_oid, video_camera_oid, item_name, timestamp)

    // Count occurrences of each item per location
    val itemCounts = dedupDatasetA.map { case (geo, _, item, _) =>
      ((geo, item), 1)
    }.reduceByKey(_ + _)

    //handle skew data replace line 17 - 19 with the following code
    
    // Introduce a salt key to handle skew before counting
    // val saltedDatasetA = dedupDatasetA.map { case (geo, cam, detection, item, ts) =>
    //   val salt = scala.util.Random.nextInt(10) // Generate random salt
    //   ((geo, salt), (cam, detection, item, ts))
    // }

    // // Count occurrences per (geo, item) with salting
    // val saltedItemCounts = saltedDatasetA.map { case ((geo, salt), (_, _, item, _)) =>
    //   ((geo, salt, item), 1)
    // }.reduceByKey(_ + _)

    // // Remove the salt before proceeding
    // val unsaltedItemCounts = saltedItemCounts.map { case ((geo, salt, item), count) =>
    //   ((geo, item), count)
    // }.reduceByKey(_ + _)

    // Sort items and assign incremental rank per geographical location
    val rankedItems = itemCounts
    .map { case ((geo, item), count) => (geo, (item, count)) }
    .groupByKey()
    .flatMap { case (geo, items) =>
      items.toList
        .sortBy { case (item, count) => (-count, item) } // Sort by count DESC, then by item name ASC for stability
        .take(topX) // Take the top X items
        .zipWithIndex // Assign rank
        .map { case ((item, count), index) => (geo, index + 1, item) } // Ensure incremental ranking
    }

    // Join with Dataset B to get geographical location name
    val joinedRDD = rankedItems
      .map { case (geo, rank, item) => (geo, (rank, item)) }
      .join(datasetB) // (geo_oid, ((rank, item), geo_location))
      .map { case (_, ((rank, item), geoLocation)) =>
        (geoLocation, rank, item)
      }

    joinedRDD
  }
}
