import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val inputFile = "file:///home/ggian/Documents/00.code/DWS-Projects/DWS102-Spark-Project/datasets/test.txt" // it will read it from args
    val spark = SparkSession.builder
      .appName("Dominance-based Queries")
      .master("local[*]")
//      .master("spark://localhost:7077")
//      .config("spark.driver.memory", "2g") // Memory for the driver
//      .config("spark.driver.memoryOverhead", "512m") // Driver memory overhead
//      .config("spark.executor.memory", "1g") // Memory per executor
//      .config("spark.executor.memoryOverhead", "512m") // Executor memory overhead
      .getOrCreate()

    val sc = spark.sparkContext

    val startTime = System.nanoTime
//    val algorithm = "baselineSkyline" // it will read it from args
//    val algorithm = "kdTreeSkyline" // it will read it from args

//    val algorithm = "baselineTopK" // it will read it from args
    val algorithm = "kdTreeTopK" // it will read it from args

    val k = 10
    algorithm match {
      case "baselineSkyline" => new BaselineSkyline(inputFile, sc)
      case "kdTreeSkyline" => new DistributedKDTreeSkyline(inputFile, sc)
      case "baselineTopK" => new BaselineTopKDominantPoints(inputFile, sc, k)
      case "kdTreeTopK" => new KDTreeTopKDominantPoints(inputFile, sc, k)
      case _ => println("Please provide a valid algorithm name.")
    }

    // this is how you can trigger the skyline class and get the results
//    val distributedKDTreeSkyline = new DistributedKDTreeSkyline(inputFile, sc)
//    val globalSkyline = distributedKDTreeSkyline.mainMemorySkyline
//    globalSkyline.foreach(point => println(point.mkString(", ")))

    val endTime = System.nanoTime - startTime
    println("Total duration of application is: " + endTime.asInstanceOf[Double] / 1000000000.0 + "second(s)")

    sc.stop()
  }
}
