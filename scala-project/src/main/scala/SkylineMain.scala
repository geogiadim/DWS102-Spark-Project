import org.apache.spark.sql.SparkSession

object SkylineMain {
  def main(args: Array[String]): Unit = {
    val inputFile = "file:///home/ggian/Documents/00.code/DWS102-Spark-Project/datasets/4d_uniform_data.txt" // it will read it from args
    val samplingRate = 0.1
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
    val algorithm = "als" // it will read it from args


    algorithm match {
      case "als" => new BaselineSkyline(inputFile, sc, samplingRate)
      case _ => println("Please provide a valid algorithm name.")
    }

    val endTime = System.nanoTime - startTime
    println("Total duration of application is: " + endTime.asInstanceOf[Double] / 1000000000.0 + "second(s)")
  }
}
