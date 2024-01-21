import org.apache.spark.SparkContext


class KDTreeSkyline(inputPath: String, sc: SparkContext) {
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

  private val points = initialDataRDD.map(_.split("\\t").map(_.toDouble)).collect()

  private val root = buildKDTree(points, depth = 0)

  private val skylinePoints = findSkylinePoints(root)

  private val localSkylinesTime = System.nanoTime()
  println("Local skyline points were calculated and retrieved in: "+(localSkylinesTime-inputTime).asInstanceOf[Double] / 1000000000.0 +" second(s)")

  println("Skyline completed. Total skylines: ", skylinePoints.length)
  skylinePoints.foreach(point => println(point.mkString(", ")))

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
    val axis = depth % k // define the dimension that will be sorted.
    val sortedPoints = points.sortBy(_(axis))

    val medianIndex = sortedPoints.length / 2
    val node = KDTreeNode(sortedPoints(medianIndex), depth = depth)
    node.left = buildKDTree(sortedPoints.slice(0, medianIndex), depth + 1)
    node.right = buildKDTree(sortedPoints.slice(medianIndex + 1, sortedPoints.length), depth + 1)

    node
  }

  def printKDTree(node: KDTreeNode): Unit = {
    if (node != null) {
      println(s"Node: ${node.point.mkString(", ")} (Depth: ${node.depth})")
      if (node.left != null) {
        println(s"Left Edge: ${node.point.mkString(", ")} -> ${node.left.point.mkString(", ")}")
      }
      if (node.right != null) {
        println(s"Right Edge: ${node.point.mkString(", ")} -> ${node.right.point.mkString(", ")}")
      }
      printKDTree(node.left)
      printKDTree(node.right)
    }
  }

  /**
   * Finds and returns all skyline points in the KD-tree.
   *
   * @return an array containing all skyline points.
   */
  def findSkylinePoints(root: KDTreeNode): Array[Array[Double]] = {
    if (root == null) return Array()

    // Initialize an empty list to store skyline points
    var skylinePoints: List[Array[Double]] = List()

    // Perform a recursive traversal of the KD-tree
    def findSkyline(node: KDTreeNode, isSkyline: Boolean, depth: Int): Unit = {
      if (node == null) return
//      skylinePoints.foreach(point => println(point.mkString(", ")))
//      println("--------")

      // Check if the current point is a skyline point
      val isCurrentSkyline = isSkylinePoint(node.point, skylinePoints)

      // If the current point is a skyline point, add it to the list
      if (isCurrentSkyline) {
        // Remove skyline points that are dominated by the new skyline point
        skylinePoints = skylinePoints.filter(existingPoint => !BasicSkyline.isDominated(node.point, existingPoint))
        skylinePoints = node.point :: skylinePoints
      }

      // Explore both subtrees
      findSkyline(node.left, isCurrentSkyline, depth + 1)
      findSkyline(node.right, isCurrentSkyline, depth + 1)

    }

    // Start the skyline query from the root with initial depth 0
    findSkyline(root, isSkyline = true, depth = 0)

    // Convert the list of skyline points to an array
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
