import org.apache.spark.SparkContext

import scala.collection.mutable


class KDTreeTopKDominantPoints (inputPath: String, sc: SparkContext, k: Int) extends Serializable {
  private val inputTime = System.nanoTime
  private val initialDataRDD = sc.textFile(inputPath)

  println("Number of INITIAL DATA RDD partitions: " + initialDataRDD.getNumPartitions)

  // build local KD-Trees in each partition and find local top-k points
  private val pointsRDD = initialDataRDD.map(_.split("\\t").map(_.toDouble))

  // build local KD-Trees in each partition and collect them
  private val partitionedTrees = pointsRDD.mapPartitions(iter => Iterator(KDTree.buildKDTree(iter.toArray, 0))).collect()

  // broadcast the array of local KD-Trees
  private val broadcastedTrees = sc.broadcast(partitionedTrees)

  // score points (parallel)
  private val scoredPoints = pointsRDD.flatMap { point =>
    broadcastedTrees.value.map { localTree =>
      var score = 0
      traverseAndScore(localTree, point, (otherPoint: Array[Double]) => {
        if (BasicSkyline.isDominated(point, otherPoint)) score += 1
      })
      (point.mkString(","), score)
    }
  }.reduceByKey(_ + _)

  // aggregate scores and find top k points
  private val aggregatedScores = scoredPoints.reduceByKey(_ + _)
  private val topKPoints = aggregatedScores.takeOrdered(k)(Ordering.by[(String, Int), Int](_._2).reverse)

  topKPoints.foreach { case (pointString, score) =>
    println(pointString + " with score: " + score)
  }

  println("Global top "+k+" dominant points were calculated in: "+(System.nanoTime-inputTime).asInstanceOf[Double] / 1000000000.0 +" second(s)")

  private def traverseAndScore(node: KDTree.KDTreeNode, point: Array[Double], action: Array[Double] => Unit): Unit = {
    if (node == null) return

    action(node.point)

    traverseAndScore(node.left, point, action)
    traverseAndScore(node.right, point, action)
  }

  private def findTopKPoints(iter: Iterator[Array[Double]]): Iterator[(Array[Double], Int, String)] = {
    if (iter.isEmpty) Iterator.empty
    else {
      val points = iter.toArray
      val localTree = KDTree.buildKDTree(points, 0)

      val localTopK: mutable.PriorityQueue[(String, Int)] =
        new mutable.PriorityQueue()(Ordering.by[(String, Int), Int](_._2).reverse)

      points.foreach { point =>
        var score = 0
        traverseAndScore(localTree, point, (otherPoint: Array[Double]) => {
          if (BasicSkyline.isDominated(point, otherPoint)) score += 1
        })
        localTopK.enqueue((point.mkString(","), score))
        if (localTopK.length > k) {
          localTopK.dequeue()
        }
      }

      // Assign a unique identifier to the subtree
      val subTreeId = java.util.UUID.randomUUID().toString
      localTopK.map { case (pointStr, score) =>
        (pointStr.split(",").map(_.toDouble), score, subTreeId)
      }.iterator
    }
  }
}


//// build local KD-Trees in each partition and find local top-k points
//private val pointsRDD = initialDataRDD.map(_.split("\\t").map(_.toDouble))
//
//// build local KD-Trees in each partition and find local top-k points
//private val localTopKPoints = pointsRDD.mapPartitions(findTopKPoints)
//localTopKPoints.collect().foreach { case (point, score, subTreeId) =>
//  println(s"Point: ${point.mkString(", ")}, Score: $score, Subtree ID: $subTreeId")
//}
//
//// Collect localTopKPoints to the driver
//private val collectedLocalTopK = localTopKPoints.collect()
//
//
//collectedLocalTopK.foreach { case (point, score, subTreeId) =>
//  var totalScore = score
//  val dominatedPoints = collection.mutable.Set.empty[Array[Double]]
//
//  collectedLocalTopK.foreach { case (otherPoint, otherScore, otherSubTreeId) =>
//    if (otherSubTreeId != subTreeId) {
//      if (!dominatedPoints.exists(dominated => BasicSkyline.isDominated(otherPoint, dominated))) {
//        if (BasicSkyline.isDominated(point, otherPoint)) {
//          totalScore += otherScore + 1
//          dominatedPoints += otherPoint
//        }
//      }
//    }
//  }
//
//  println(s"Point: ${point.mkString(", ")}, Updated Score: $totalScore, Subtree ID: $subTreeId")
//}


//def findGlobalTopKPoints(node: KDTree.KDTreeNode, k: Int): List[(Array[Double], Int)] = {
//  if (node == null) return List.empty
//
//  val globalTopK: mutable.PriorityQueue[(Array[Double], Int)] =
//    new mutable.PriorityQueue()(Ordering.by[(Array[Double], Int), Int](_._2).reverse)
//
//  def calc(node: KDTree.KDTreeNode, k: Int): Unit = {
//    if (node == null) return
//    val currentNodeSubTreeId = collectedLocalTopK.find(_._1.sameElements(node.point))
//      .map(_._3)
//      .getOrElse("")
//
//
//    collectedLocalTopK.foreach { case (point, localScore, subTreeId) =>
//      if (currentNodeSubTreeId != subTreeId) {
//        var score = localScore
//
//        traverseAndScore(node, point, (otherPoint: Array[Double]) => {
//          if (BasicSkyline.isDominated(point, otherPoint)) score += 1
//        })
//
//        globalTopK.enqueue((point, score))
//        if (globalTopK.length > k) {
//          globalTopK.dequeue()
//        }
//      }
//    }
//
//    calc(node.left, k)
//    calc(node.right, k)
//  }
//  calc(node, k)
//
//  // return
//  globalTopK.toList
//}






