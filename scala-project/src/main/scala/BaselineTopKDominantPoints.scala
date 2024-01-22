import org.apache.spark.SparkContext


class BaselineTopKDominantPoints (inputPath: String, sc: SparkContext, k: Int) extends Serializable {
  private val inputTime = System.nanoTime
  private val initialDataRDD = sc.textFile(inputPath)

  println("Number of INITIAL DATA RDD partitions: " + initialDataRDD.getNumPartitions)

  private val points = initialDataRDD.map(line => line.split("\\t").map(_.toDouble))
  private val broadcastedPoints = sc.broadcast(points.collect())
  private val scores = points.map { point =>
    val score = broadcastedPoints.value.count(otherPoint => BasicSkyline.isDominated(point, otherPoint))
    (point, score)
  }

  private val topK = scores.sortBy(-_._2).take(k)
  topK.foreach(point => println(point._1.mkString(", ") + " with score: " + point._2))

  println("Global top "+k+" dominant points were calculated in: "+(System.nanoTime-inputTime).asInstanceOf[Double] / 1000000000.0 +" second(s)")

}
//
//0.1, 0.2 with score: 8
//0.4, 0.3 with score: 4
//0.2, 0.6 with score: 4
//0.3, 0.5 with score: 3
//0.6, 0.4 with score: 2
//0.2, 0.8 with score: 1
//0.7, 0.6 with score: 1
//0.8, 0.1 with score: 1
//0.9, 0.7 with score: 0
//0.5, 0.9 with score: 0