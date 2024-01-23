package TopKQuery

import Skyline.DistributedKDTreeSkyline
import TopKInSkyline.KDTreeTopKSkyline
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Implements the Skyline-Based Top-K Dominating (STD) algorithm.
 *
 * @param sc spark context is needed for topKinSkyline class
 * @param initialDataRDD the dataset in which the algorithm is executed
 */
class STDQuery(sc:SparkContext, initialDataRDD: RDD[Array[Double]]) extends Serializable {
  private val distributedKDTreeSkyline = new DistributedKDTreeSkyline()
  private val task3 = new KDTreeTopKSkyline(sc, initialDataRDD)

  /**
   * Calculates the top-1 point in skyline and add it in the returning list.
   * Then removes this point from initial dataset.
   * Calculates new skyline in the remaining dataset.
   * Repeats this process for k times
   *
   * @param globalSkyline the global skyline
   * @param k defines the number of top returned points
   * @return the top k points
   */
  def calculate(globalSkyline: Array[Array[Double]], k: Int): Array[(Array[Double], Int)] = {
    var topKDominatingPoints: Array[(Array[Double], Int)] = Array()
    var exclusiveRegionSkyline = globalSkyline
    var remainingPointsRDD: RDD[Array[Double]] = initialDataRDD
    for (_ <- 1 to k) {
      // Find the point with the highest dominance score (top-1 dominating)
      val top1DominatingPoint = task3.calculate(exclusiveRegionSkyline, 1)

      // Remove top-1 dominating point from the dataset
      remainingPointsRDD = remainingPointsRDD.filter(point => !point.sameElements(top1DominatingPoint.head._1))

      // Find new skyline in the exclusive region of the selected point
      exclusiveRegionSkyline = distributedKDTreeSkyline.calculate(remainingPointsRDD)

      // Add new skyline points to the list
      topKDominatingPoints = topKDominatingPoints ++ top1DominatingPoint
    }

    topKDominatingPoints
  }
}
