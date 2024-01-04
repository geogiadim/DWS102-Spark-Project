import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object TopKDominantPointsGlobal {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Top K Dominant Points").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val data = sc.textFile("/home/michalis/IdeaProjects/DWS102-Spark-Project/datasets/3d_uniform_data.txt")
    val points = data.map(line => line.split("\\t").map(_.toDouble))

    // broadcast the entire dataset to each node
    val broadcastedPoints = sc.broadcast(points.collect())

    // calculate the dominance score for each point
    val scores = points.map { point =>
      val score = broadcastedPoints.value.count(otherPoint => isDominated(otherPoint, point))
      (point, score)
    }

    // find the top-k dominant points
    val topK = scores.sortBy(-_._2).take(10)

    topK.foreach(point => println(point._1.mkString(", ") + " with score: " + point._2))

    sc.stop()
  }

  def isDominated(x: Array[Double], y: Array[Double]): Boolean = {
    x.zip(y).exists {
      case (xi, yi) => xi > yi
    } && !x.zip(y).exists {
      case (xi, yi) => xi < yi
    }
  }
}
