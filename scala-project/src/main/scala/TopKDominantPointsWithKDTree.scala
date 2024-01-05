import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.mutable.PriorityQueue

case class KDTreeNode(point: Array[Double], var left: KDTreeNode = null, var right: KDTreeNode = null)

object TopKDominantPointsWithKDTree {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Top K Dominant Points with KD-Tree").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val data = sc.textFile("/home/michalis/IdeaProjects/DWS102-Spark-Project/datasets/3d_uniform_data.txt")
    val points = data.map(_.split("\\t").map(_.toDouble)).collect()
    val k = 10 // return tok k points to find

    val root = buildKDTree(points, depth = 0)

    val topKQueue = findDominantPoints(points, root, k)

    // sort by score in descending order
    val sortedTopK = topKQueue.toList.sortBy(-_._2)

    sortedTopK.foreach { case (point, score) =>
      println(point.mkString(", ") + " with score: " + score)
    }

    sc.stop()
  }

  def buildKDTree(points: Array[Array[Double]], depth: Int): KDTreeNode = {
    if (points.isEmpty) return null

    val k = points(0).length
    val axis = depth % k
    val sortedPoints = points.sortBy(_(axis))

    val medianIndex = sortedPoints.length / 2
    val node = KDTreeNode(sortedPoints(medianIndex))
    node.left = buildKDTree(sortedPoints.slice(0, medianIndex), depth + 1)
    node.right = buildKDTree(sortedPoints.slice(medianIndex + 1, sortedPoints.length), depth + 1)

    node
  }

  def findDominantPoints(allPoints: Array[Array[Double]], node: KDTreeNode, k: Int, depth: Int = 0): PriorityQueue[(Array[Double], Int)] = {
    val queue = PriorityQueue.empty[(Array[Double], Int)](Ordering.by(-_._2))

    allPoints.foreach { point =>
      var score = 0
      traverseAndScore(node, point, (otherPoint: Array[Double]) => {
        if (isDominated(point, otherPoint)) score += 1
      })
      queue.enqueue((point, score))

      if (queue.size > k) {
        queue.dequeue()
      }
    }

    queue
  }

  def traverseAndScore(node: KDTreeNode, point: Array[Double], action: Array[Double] => Unit): Unit = {
    if (node == null) return

    action(node.point)

    traverseAndScore(node.left, point, action)
    traverseAndScore(node.right, point, action)
  }

  def isDominated(x: Array[Double], y: Array[Double]): Boolean = {
    x.zip(y).exists {
      case (xi, yi) => xi < yi
    } && !x.zip(y).exists {
      case (xi, yi) => xi > yi
    }
  }
}
