package Skyline

import KDTree.KDTree
import org.apache.spark.rdd.RDD

/**
 * This class implements the skyline algorithm using a KD-Tree.
 */
class DistributedKDTreeSkyline() extends Serializable {
  /**
   * Calculates the Skyline given a dataset with d-dimensional points.
   *
   * @param initialDataRDD the dataset in which the algorithm is executed
   * @return the skyline
   */
  def calculate(initialDataRDD: RDD[Array[Double]]): Array[Array[Double]]  = {
    // Build KD-trees in parallel and find local skyline for each sub-tree
    val localSkylinesRDD = initialDataRDD.mapPartitions(iter => Iterator(KDTree.buildKDTree(iter.toArray, 0)))
                                         .flatMap(findSkylinePoints)

    // Assume that localSkylines fit in driver's main memory
    val root = KDTree.buildKDTree(localSkylinesRDD.collect(), depth = 0)
    val mainMemorySkyline: Array[Array[Double]] = findSkylinePoints(root)

    mainMemorySkyline
  }

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
