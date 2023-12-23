import org.apache.spark.sql.SparkSession
import org.apache.log4j._


object WordCount {
  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org.apache.spark.SparkContext").setLevel(Level.WARN)
//   if (args.length != 1) {
//     println("Usage: WordCount <path-to-input-file>")
//     System.exit(1)
//   }
//   val inputFile = args(0)
    val currentDir = System.getProperty("user.dir")  // get the current directory
    println(currentDir)
//    val inputFile = "file:///opt/bitnami/spark/datafiles/normal_data.txt"
    val inputFile = "file://" + currentDir + "/normal_data.txt"
//    val inputFile = "file:///normal_data.txt"
    println(inputFile)

    // Create a Spark session
    val spark = SparkSession.builder
      .appName("WordCount")
      .master("spark://172.28.0.2:7077")  // replace with spark-master ip address
      .getOrCreate()

    val textRDD = spark.sparkContext.textFile(inputFile)
    val linesCollected = textRDD.collect()
    linesCollected.foreach(println)

    //    val result = textRDD
//      .flatMap(line => line.split(" "))
//      .map(word => (word, 1))
//      .reduceByKey((count1, count2) => count1 + count2)
//      .collect()
//      .foreach(println)

    // Stop the Spark session
    spark.stop()
  }
}