import org.apache.spark.SparkContext

class KDTreeTopKSkyline(inputPath: String, sc: SparkContext, k: Int) extends Serializable {
  private val inputTime = System.nanoTime

  // global skyline from DistributedKDTreeSkyline
  private val distributedKDTreeSkyline = new DistributedKDTreeSkyline(inputPath, sc)
  private val globalSkyline = distributedKDTreeSkyline.mainMemorySkyline

  // build local KD-Trees in each partition for the input dataset
  private val initialDataRDD = sc.textFile(inputPath)
  private val pointsRDD = initialDataRDD.map(_.split("\\t").map(_.toDouble))
  private val partitionedTrees = pointsRDD.mapPartitions(iter => Iterator(KDTree.buildKDTree(iter.toArray, 0))).collect()

  // broadcast the array of local KD-Trees
  private val broadcastedTrees = sc.broadcast(partitionedTrees)

  // score global skyline points
  private val scoredSkylinePoints = globalSkyline.flatMap { skylinePoint =>
    broadcastedTrees.value.map { localTree =>
      var score = 0
      traverseAndScore(localTree, skylinePoint, (otherPoint: Array[Double]) => {
        if (BasicSkyline.isDominated(skylinePoint, otherPoint)) score += 1
      })
      (skylinePoint.mkString(","), score)
    }
  }.groupBy(_._1).mapValues(_.map(_._2).sum)

  // find the top k points
  private val topKSkylinePoints = scoredSkylinePoints.toList.sortBy(-_._2).take(k)

  topKSkylinePoints.foreach { case (pointString, score) =>
    println(pointString + " with score: " + score)
  }

  println("Top "+k+" dominant skyline points were calculated in: "+(System.nanoTime-inputTime).asInstanceOf[Double] / 1000000000.0 +" second(s)")

  private def traverseAndScore(node: KDTree.KDTreeNode, point: Array[Double], action: Array[Double] => Unit): Unit = {
    if (node == null) return
    action(node.point)
    traverseAndScore(node.left, point, action)
    traverseAndScore(node.right, point, action)
  }
}
