package TopKQuery

import Skyline.BasicSkyline
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Baseline implementation of top-k dominating points given a dataset with d-dimensional points.
 *
 * @param sc spark context is used to broadcast the data to workers
 */
class BaselineTopKQuery(sc: SparkContext) extends Serializable {
  def calculate(initialDataRDD: RDD[Array[Double]], k: Int): Array[(Array[Double], Int)] = {
    val broadcastedPoints = sc.broadcast(initialDataRDD.collect())
    val scores = initialDataRDD.map { point =>
      val score = broadcastedPoints.value.count(otherPoint => BasicSkyline.isDominated(point, otherPoint))
      (point, score)
    }
    val topK = scores.sortBy(-_._2).take(k)
    topK
  }
}
