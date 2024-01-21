import org.apache.spark.sql.SparkSession

object SkylineMain {
  def main(args: Array[String]): Unit = {
    val inputFile = "file:///home/ggian/Documents/00.code/DWS-Projects/DWS102-Spark-Project/datasets/anticorrelated_data.txt" // it will read it from args
    val spark = SparkSession.builder
      .appName("Skyline")
      .master("local[*]")
//      .master("spark://localhost:7077")
//      .config("spark.driver.memory", "2g") // Memory for the driver
//      .config("spark.driver.memoryOverhead", "512m") // Driver memory overhead
//      .config("spark.executor.memory", "1g") // Memory per executor
//      .config("spark.executor.memoryOverhead", "512m") // Executor memory overhead
      .getOrCreate()

    val sc = spark.sparkContext

    val startTime = System.nanoTime
    val algorithm = "baselineSkyline" // it will read it from args
//    val algorithm = "kdTreeSkyline" // it will read it from args
//    val algorithm = "distributedKDTreeSkyline" // it will read it from args

    algorithm match {
      case "baselineSkyline" => new BaselineSkyline(inputFile, sc)
      case "kdTreeSkyline" => new DistributedKDTreeSkyline(inputFile, sc)
      case _ => println("Please provide a valid algorithm name.")
    }

    val endTime = System.nanoTime - startTime
    println("Total duration of application is: " + endTime.asInstanceOf[Double] / 1000000000.0 + "second(s)")

    sc.stop()
  }
}
