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
//0.1,0.2 with score: 8
//0.2,0.6 with score: 4
//0.4,0.3 with score: 4
//0.3,0.5 with score: 3
//0.6,0.4 with score: 2
//0.7,0.6 with score: 1
//0.2,0.8 with score: 1
//0.8,0.1 with score: 1
//0.9,0.7 with score: 0
//0.5,0.9 with score: 0

//0.4493993993993994,0.3988122736861217 with score: 166
//0.28298298298298297,0.5628012031352508 with score: 165
//0.36286286286286284,0.48572725707019715 with score: 160
//0.7413413413413413,0.10372927123085218 with score: 148
//0.2373373373373373,0.6293851663403837 with score: 139
//0.4028028028028028,0.4745708688577356 with score: 129
//0.5654154154154154,0.3205091182598188 with score: 129
//0.15840840840840842,0.7294475928198302 with score: 126
//0.2753753753753754,0.612124465911436 with score: 121
//0.4408408408408408,0.44947239315223075 with score: 119
//Global top 10 dominant points were calculated in: 1.124100152 second(s)