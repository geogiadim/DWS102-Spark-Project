package KDTree

object KDTree {
  /**
   * Represents a node in the KD-tree.
   * Contains a point (represented as an array of doubles),
   * references to the left and right child nodes,
   * and the depth of the node in the tree.
   */
  case class KDTreeNode(point: Array[Double], var left: KDTreeNode = null, var right: KDTreeNode = null, var depth: Int = 0)

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

  def traverseAndScore(node: KDTree.KDTreeNode, point: Array[Double], action: Array[Double] => Unit): Unit = {
    if (node == null) return
    action(node.point)
    traverseAndScore(node.left, point, action)
    traverseAndScore(node.right, point, action)
  }
}
