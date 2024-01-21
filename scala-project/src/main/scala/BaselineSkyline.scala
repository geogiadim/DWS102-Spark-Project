import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

/**
 * This is a baseline implementation of Skyline Algorithm. It reads a datafile in RDD.
 * Then it calculates all Local Skylines in each partition of the RDD.
 * Then it assumes that the union of the local skylines fits in driver's memory and collect them there.
 * Finally it calculates the global skyline.
 * This implementation is not appropriate for big datafiles as well as it calculates the local skylines comparing all to all data points.
 * Moreover, for big datafiles collecting all local skylines may cause main memory overflow.
 **/
class BaselineSkyline(inputPath: String, sc: SparkContext) extends Serializable {
  private val inputTime = System.nanoTime
  private val initialDataRDD = sc.textFile(inputPath)

  println("Number of INITIAL DATA RDD partitions: " + initialDataRDD.getNumPartitions)

  private val localSkylinesRDD = initialDataRDD.map(x=>x.split("\t"))
                                            .map(x => x.map(y => y.toDouble))
                                            .mapPartitions(SFSSkyline.calculate)

  println("Number of LOCAL SKYLINES RDD partitions: " + localSkylinesRDD.getNumPartitions)
  println("Number of initial data points: " + initialDataRDD.count())
  println("Number of local skylines: " + localSkylinesRDD.count())
  localSkylinesRDD.persist(StorageLevel.MEMORY_AND_DISK)

  // Use mapPartitionsWithIndex to print each line along with its partition index
//  localSkylinesRDD.mapPartitionsWithIndex { (index, iterator) =>
//    iterator.map(localSkyline => s"Partition $index: ${localSkyline.mkString(" ")}")
//  }.collect().foreach(println)

  private val localSkylinesTime = System.nanoTime()
  println("Local skyline points were calculated and retrieved in: "+(localSkylinesTime-inputTime).asInstanceOf[Double] / 1000000000.0 +" second(s)")

  // Assume that localSkylines fit in driver's main memory
  private val mainMemorySkyline = SFSSkyline.calculate(localSkylinesRDD.collect().toIterator).toList
//  println("Skyline completed. Total skylines: " + mainMemorySkyline.length)
  mainMemorySkyline.foreach(skyline => println(skyline.mkString(" ")))

  println("Global skyline points were calculated in: "+(System.nanoTime-localSkylinesTime).asInstanceOf[Double] / 1000000000.0 +" second(s)")
}
