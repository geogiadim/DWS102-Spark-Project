import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.mutable.PriorityQueue

case class KDTreeNode(point: Array[Double], var left: KDTreeNode = null, var right: KDTreeNode = null)

object TopKDominantPointsWithKDTree {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Top K Dominant Points with KD-Tree").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val data = sc.textFile("/home/michalis/IdeaProjects/DWS102-Spark-Project/datasets/3d_uniform_data.txt")
    val pointsRDD = data.map(_.split("\\t").map(_.toDouble))

    // top k points
    val k = 10

    // build local KD-Trees in each partition and collect them
    val partitionedTrees = pointsRDD.mapPartitions { partition =>
      if (partition.isEmpty) Iterator.empty
      else {
        val points = partition.toArray
        val localTree = buildKDTree(points, 0)
        Iterator(localTree)
      }
    }.collect()

    // broadcast the array of local KD-Trees
    val broadcastedTrees = sc.broadcast(partitionedTrees)

    // score points (parallel)
    val scoredPoints = pointsRDD.flatMap { point =>
      broadcastedTrees.value.map { localTree =>
        var score = 0
        traverseAndScore(localTree, point, (otherPoint: Array[Double]) => {
          if (isDominated(point, otherPoint)) score += 1
        })
        (point.mkString(","), score)
      }
    }.reduceByKey(_ + _)

    // aggregate scores and find top k points
    val aggregatedScores = scoredPoints.reduceByKey(_ + _)
    val topKPoints = aggregatedScores.takeOrdered(k)(Ordering.by[(String, Int), Int](_._2).reverse)

    topKPoints.foreach { case (pointString, score) =>
      println(pointString + " with score: " + score)
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
