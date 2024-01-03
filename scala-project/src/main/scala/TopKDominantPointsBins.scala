import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object TopKDominantPointsBins {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Top K Dominant Points").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val data = sc.textFile("/home/michalis/IdeaProjects/DWS102-Spark-Project/datasets/4d_anticorrelated_data_10k.txt")
    val points = data.map(line => line.split("\\t").map(_.toDouble))

    // define the number of bins for each dimension
    val numBinsPerDimension = 10

    // partition the space and assign points to bins
    val binnedPoints = points.map(point => (bin(point, numBinsPerDimension), point))

    // group points by bin and calculate dominance within each bin
    val scoresByBin = binnedPoints.groupByKey().flatMap {
      case (_, pointsInBin) => calculateDominanceScores(pointsInBin)
    }

    // find top-k dominant points
    val topK = scoresByBin.sortBy(-_._2).take(10)

    topK.foreach(point => println(point._1.mkString(", ") + " with score: " + point._2))

    sc.stop()
  }

  // determine the bin for a point in multi-dimensional space
  def bin(point: Array[Double], numBins: Int): String = {
    point.map(coord => (coord * numBins).toInt).mkString("-")
  }

  // calculate dominance scores within a bin
  def calculateDominanceScores(points: Iterable[Array[Double]]): Iterable[(Array[Double], Int)] = {
    // calculate scores within each bin
    points.map { point =>
      val score = points.count(otherPoint => isDominated(otherPoint, point))
      (point, score)
    }
  }

  def isDominated(x: Array[Double], y: Array[Double]): Boolean = {
    x.zip(y).forall { case (xi, yi) => xi <= yi } && x.zip(y).exists { case (xi, yi) => xi < yi }
  }
}
