import org.apache.spark.{SparkConf, SparkContext}

object SkylineSet {
  def main(args: Array[String]): Unit = {
    // Create a Spark configuration
    val conf = new SparkConf().setAppName("SkylineSet")

    // Create a Spark context
    val sc = new SparkContext(conf)

    // Replace the following with your actual input file path
    val inputFile = "file://" + currentDir + "datafiles/2d/1k-points/normal_data.txt"

    // Read the input file and create an RDD of d-dimensional points
    val pointsRDD = sc.textFile(inputFile)
      .map(line => line.split("\t").map(_.toDouble))
      .map(point => (point, 1)) // Add a dummy value to each point for later filtering

    // Find the skyline set using the dominance check
    val skylineSet = pointsRDD.filter(point => !isDominated(point, pointsRDD))

    // Print the skyline set
    skylineSet.map(point => point._1.mkString(",")).foreach(println)

    // Stop the Spark context
    sc.stop()
  }

  // Function to check if a point is dominated by any other point in the RDD
  def isDominated(point: (Array[Double], Int), pointsRDD: org.apache.spark.rdd.RDD[(Array[Double], Int)]): Boolean = {
    pointsRDD.filter(other => dominates(other._1, point._1)).count() > 0
  }

  // Function to check if one point dominates another
  def dominates(point1: Array[Double], point2: Array[Double]): Boolean = {
    point1.zip(point2).forall { case (x, y) => x <= y } && point1.exists(_ < point2.head)
  }
}
