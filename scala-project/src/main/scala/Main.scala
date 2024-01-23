import Skyline.DistributedKDTreeSkyline
import TopKQuery.STDQuery
import org.apache.spark.sql.SparkSession
import java.io.{BufferedWriter, File, FileWriter}

object Main {
  def main(args: Array[String]): Unit = {
    val outputPath = "/home/ggian/log.txt"
    val file = new File(outputPath)
    val writer = new BufferedWriter(new FileWriter(file))

    val providedPath = "/home/ggian/Documents/00.code/DWS-Projects/DWS102-Spark-Project/datasets/test.txt"
    val inputFile = "file://"+providedPath // it will read it from args
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
    val initialDataRDD = sc.textFile(inputFile).map(_.split("\\t").map(_.toDouble))
    try {
      writer.write("#####################\n")
      writer.write("#####################\n")
      writer.write("Number of INITIAL DATA RDD partitions: " + initialDataRDD.getNumPartitions +"\n")
      writer.write("#####################\n")
      writer.write("#####################\n")

      val startTime = System.nanoTime
      val k = 10 // it will read it from args

      // Skyline
      writer.write("\n##### Skyline Query #####\n")
      val skylineTime = System.nanoTime
      val distributedKDTreeSkyline = new DistributedKDTreeSkyline()
      val globalSkyline = distributedKDTreeSkyline.calculate(initialDataRDD)
      writer.write("Skyline points were calculated in: "+(System.nanoTime-skylineTime).asInstanceOf[Double] / 1000000000.0 +" second(s)\n")
      writer.write("Skyline completed. Total skylines: " + globalSkyline.length +"\n\n")
      globalSkyline.foreach { point =>
        val output = point.mkString(", ")
        writer.write(output+"\n")
      }
      writer.write("#####################\n")

      // top-k query
      writer.write("\n##### Top-K Dominating Points Query #####\n")
      val topKTime = System.nanoTime
      val topkQuery = new STDQuery(sc, initialDataRDD)
      val topKPoints = topkQuery.calculate(globalSkyline, k)
      writer.write("Top "+k+" dominating points were calculated in: "+(System.nanoTime-topKTime).asInstanceOf[Double] / 1000000000.0 +" second(s)\n\n")
      topKPoints.foreach { case (point, score) =>
        writer.write(s"Point: ${point.mkString(", ")}, Score: $score\n")
      }
      writer.write("#####################\n")

      // top-k in skyline
      writer.write("\n##### Top-K Dominating Points in Skyline Query #####\n")
      val topKInSkylineTime = System.nanoTime
      val topKInSkyline = new TopKInSkyline.KDTreeTopKSkyline(sc, initialDataRDD)
      val results = topKInSkyline.calculate(globalSkyline, k)
      writer.write("Top "+k+" dominating points in Skyline were calculated in: "+(System.nanoTime-topKInSkylineTime).asInstanceOf[Double] / 1000000000.0 +" second(s)\n\n")
      results.foreach { case (point, score) =>
        writer.write(s"Point: ${point.mkString(", ")}, Score: $score\n")
      }
      writer.write("#####################\n")


      val endTime = System.nanoTime - startTime
      writer.write("\n#####################")
      writer.write("\n#####################")
      writer.write("\nTotal duration of application is: " + endTime.asInstanceOf[Double] / 1000000000.0 + "second(s)")
      writer.write("\n#####################")
      writer.write("\n#####################")
    } finally {
      writer.close()
    }
    sc.stop()
  }
}
