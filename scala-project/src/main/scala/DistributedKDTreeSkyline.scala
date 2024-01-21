import org.apache.spark.SparkContext


class DistributedKDTreeSkyline(inputPath: String, sc: SparkContext) extends Serializable {
  /**
   * Represents a node in the KD-tree.
   * Contains a point (represented as an array of doubles),
   * references to the left and right child nodes,
   * and the depth of the node in the tree.
   */
  case class KDTreeNode(point: Array[Double], var left: KDTreeNode = null, var right: KDTreeNode = null, var depth: Int = 0)

  private val inputTime = System.nanoTime
  private val initialDataRDD = sc.textFile(inputPath)

  println("Number of INITIAL DATA RDD partitions: " + initialDataRDD.getNumPartitions)

  // Build KD-trees in parallel and find local skyline for each sub-tree
  private val localSkylinesRDD = initialDataRDD.map(_.split("\\t").map(_.toDouble))
                                            .mapPartitions(iter => Iterator(buildKDTree(iter.toArray, 0)))
                                            .flatMap(findSkylinePoints)

  println("Number of LOCAL SKYLINES RDD partitions: " + localSkylinesRDD.getNumPartitions)
  println("Number of initial data points: " + initialDataRDD.count())
  println("Number of local skylines: " + localSkylinesRDD.count())

  private val localSkylinesTime = System.nanoTime()
  println("Local skyline points were calculated and retrieved in: "+(localSkylinesTime-inputTime).asInstanceOf[Double] / 1000000000.0 +" second(s)")

  // Assume that localSkylines fit in driver's main memory
  private val root = buildKDTree(localSkylinesRDD.collect(), depth = 0)
  private val mainMemorySkyline = findSkylinePoints(root)
//  println("Skyline completed. Total skylines: " + mainMemorySkyline.length)
  mainMemorySkyline.foreach(point => println(point.mkString(", ")))

  println("Global skyline points were calculated in: "+(System.nanoTime-localSkylinesTime).asInstanceOf[Double] / 1000000000.0 +" second(s)")

  /**
   * Constructs a KD-tree recursively from a given set of points.
   * The function takes an array of points (points) and the current depth (depth) as input.
   * The points are sorted based on the axis determined by the current depth mod the number of dimensions (k).
   * This approach helps in achieving a well-balanced KD-tree, which is important for maintaining its effectiveness in various spatial queries.
   * The median point along the sorted axis is chosen as the node, and the left and right subtrees are constructed
   * recursively using the points on the left and right of the median.
   *
   * @return the root node of the constructed KD-tree.
   */
  def buildKDTree(points: Array[Array[Double]], depth: Int): KDTreeNode = {
    if (points.isEmpty) return null

    val k = points(0).length
    val axis = depth % k
    val sortedPoints = points.sortBy(_(axis))

    val medianIndex = sortedPoints.length / 2
    val node = KDTreeNode(sortedPoints(medianIndex), depth = depth)
    node.left = buildKDTree(sortedPoints.slice(0, medianIndex), depth + 1)
    node.right = buildKDTree(sortedPoints.slice(medianIndex + 1, sortedPoints.length), depth + 1)

    node
  }

  /**
   * Finds and returns all skyline points in the KD-tree.
   *
   * @return an array containing all skyline points.
   */
  def findSkylinePoints(root: KDTreeNode): Array[Array[Double]] = {
    if (root == null) return Array()

    var skylinePoints: List[Array[Double]] = List()

    def findSkyline(node: KDTreeNode, isSkyline: Boolean, depth: Int): Unit = {
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
  def isSkylinePoint(point: Array[Double], skylinePoints: List[Array[Double]]): Boolean = {
    skylinePoints.forall(existingPoint => !BasicSkyline.isDominated(existingPoint, point))
  }

}
