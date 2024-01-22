import org.apache.spark.SparkContext


class DistributedKDTreeSkyline(inputPath: String, sc: SparkContext) extends Serializable {
  private val inputTime = System.nanoTime
  private val initialDataRDD = sc.textFile(inputPath)

  println("Number of INITIAL DATA RDD partitions: " + initialDataRDD.getNumPartitions)

  // Build KD-trees in parallel and find local skyline for each sub-tree
  private val localSkylinesRDD = initialDataRDD.map(_.split("\\t").map(_.toDouble))
                                            .mapPartitions(iter => Iterator(KDTree.buildKDTree(iter.toArray, 0)))
                                            .flatMap(findSkylinePoints)

  println("Number of LOCAL SKYLINES RDD partitions: " + localSkylinesRDD.getNumPartitions)
  println("Number of initial data points: " + initialDataRDD.count())
  println("Number of local skylines: " + localSkylinesRDD.count())

  private val localSkylinesTime = System.nanoTime()
  println("Local skyline points were calculated and retrieved in: "+(localSkylinesTime-inputTime).asInstanceOf[Double] / 1000000000.0 +" second(s)")

  // Assume that localSkylines fit in driver's main memory
  private val root = KDTree.buildKDTree(localSkylinesRDD.collect(), depth = 0)
  val mainMemorySkyline: Array[Array[Double]] = findSkylinePoints(root)
  println("Skyline completed. Total skylines: " + mainMemorySkyline.length)
//  mainMemorySkyline.foreach(point => println(point.mkString(", ")))

  println("Global skyline points were calculated in: "+(System.nanoTime-localSkylinesTime).asInstanceOf[Double] / 1000000000.0 +" second(s)")

  /**
   * Finds and returns all skyline points in the KD-tree.
   *
   * @return an array containing all skyline points.
   */
  private def findSkylinePoints(root: KDTree.KDTreeNode): Array[Array[Double]] = {
    if (root == null) return Array()

    var skylinePoints: List[Array[Double]] = List()

    def findSkyline(node: KDTree.KDTreeNode, isSkyline: Boolean, depth: Int): Unit = {
      if (node == null) return

      val isCurrentSkyline = isSkylinePoint(node.point, skylinePoints)

      if (isCurrentSkyline) {
        skylinePoints = skylinePoints.filter(existingPoint => !BasicSkyline.isDominated(node.point, existingPoint))
        skylinePoints = node.point :: skylinePoints
      }

      findSkyline(node.left, isCurrentSkyline, depth + 1)
      findSkyline(node.right, isCurrentSkyline, depth + 1)
    }

    findSkyline(root, isSkyline = true, depth = 0)
    skylinePoints.reverse.toArray
  }

  /**
   * Checks if a point is a skyline point by comparing it with existing skyline points.
   *
   * @return true if the point is a skyline point, false otherwise.
   */
  private def isSkylinePoint(point: Array[Double], skylinePoints: List[Array[Double]]): Boolean = {
    skylinePoints.forall(existingPoint => !BasicSkyline.isDominated(existingPoint, point))
  }

}
