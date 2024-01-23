package Skyline

import Skyline.BasicSkyline.isDominated
import scala.collection.mutable.ArrayBuffer

object SFSSkyline extends Serializable {
  def calculate(data: Iterator[Array[Double]]): Iterator[Array[Double]] = {
    val data_with_score = addScore(data)
    val sorted_data = sortByScore(data_with_score)
    // param function extracts the Array[Double] part.
    val result = _calculate(sorted_data.map(x => x._1))
    result
  }

  // This function transforms an iterator of arrays into an iterator of tuples,
  // where each tuple contains the original array and a sum computed based on the logarithm of each element in the array.
  private def addScore(data: Iterator[Array[Double]]): Iterator[(Array[Double], Double)] = {
    // The + 1 is added inside the logarithm to avoid issues when the element is zero, as the logarithm of zero is undefined.
    data.map { x =>
      val sum = x.map(element => math.log(element + 1)).sum
      (x, sum)
    }
  }

  // This function takes an iterator of tuples, where each tuple contains an Array[Double] and a Double,
  // and sorts the tuples in descending order (-x._2) based on the second element of the tuple (the Double)
  private def sortByScore(iterator: Iterator[(Array[Double], Double)]): Iterator[(Array[Double], Double)] = {
    iterator.toSeq.sortBy(x => -x._2).toIterator
  }

  private def _calculate(x: Iterator[Array[Double]]): Iterator[Array[Double]] = {
    val sorted_data = x.toArray
//    println(sorted_data(0).mkString("Array(", ", ", ")"))
    val skyline = ArrayBuffer(sorted_data(0))

    for (i <- 1 until sorted_data.length) {
      var j = 0
      var shouldAdd = true

      while (j < skyline.length) {
        if (isDominated(sorted_data(i), skyline(j))) {
          skyline.remove(j)
          j -= 1
        } else if (isDominated(skyline(j), sorted_data(i))) {
          shouldAdd = false
        }
        j += 1
      }

      if (shouldAdd)
        skyline += sorted_data(i)
    }

    skyline.toIterator
  }
}
