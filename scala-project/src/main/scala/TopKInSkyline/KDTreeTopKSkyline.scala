package TopKInSkyline

import Skyline.BasicSkyline
import KDTree.KDTree
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Implements the algorithm TopK dominating points in Skyline.
 *
 * @param sc spark context is used to broadcast the data to workers
 * @param initialDataRDD the dataset in which the algorithm is executed
 */
class KDTreeTopKSkyline(sc: SparkContext, initialDataRDD: RDD[Array[Double]]) extends Serializable {
  /**
   * Calculates the TopK dominating points for a given skyline and the initial dataset
   *
   * @param globalSkyline the skyline set
   * @param k defines the number of top returned points
   * @return the top k points in skyline
   */
  def calculate(globalSkyline: Array[Array[Double]], k: Int): List[(Array[Double], Int)] = {
    // build local KD-Trees in each partition for the input dataset
    val partitionedTrees = initialDataRDD.mapPartitions(iter => Iterator(KDTree.buildKDTree(iter.toArray, 0))).collect()
    // broadcast the array of local KD-Trees
    val broadcastedTrees = sc.broadcast(partitionedTrees)

    // score global skyline points
    val scoredSkylinePoints = globalSkyline.flatMap { skylinePoint =>
      broadcastedTrees.value.map { localTree =>
        var score = 0
        KDTree.traverseAndScore(localTree, skylinePoint, (otherPoint: Array[Double]) => {
          if (BasicSkyline.isDominated(skylinePoint, otherPoint)) score += 1
        })
        (skylinePoint, score)
      }
    }.groupBy(_._1).mapValues(_.map(_._2).sum)

    // find the top k points
    val topKSkylinePoints: List[(Array[Double], Int)] = scoredSkylinePoints.toList.sortBy(-_._2).take(k)
    topKSkylinePoints
  }
}

//    val scoredSkylinePoints2 = globalSkyline.flatMap { skylinePoint =>
//      broadcastedTrees.value.map { localTree =>
//        var score = 0
//        KDTree.traverseAndScore(localTree, skylinePoint, (otherPoint: Array[Double]) => {
//          if (BasicSkyline.isDominated(skylinePoint, otherPoint)) score += 1
//        })
//        (skylinePoint, score)
//      }
//    }.groupBy(_._1).mapValues(_.map(_._2).sum)
//
//    val topSkylinePoint: Array[Double] = scoredSkylinePoints2.toList.sortBy(-_._2).head._1
//    topSkylinePoint

//    topKSkylinePoints.foreach { case (pointString, score) =>
//      println(pointString + " with score: " + score)
//    }
//