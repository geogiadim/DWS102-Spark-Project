import scala.collection.mutable.ArrayBuffer
import SkylineCalculation.isDominated

object SFSSkylineCalculation extends Serializable {
  def addScoreAndCalculate(x: Iterator[Array[Double]]): Iterator[Array[Double]] = {
    val y = addScoringFunction(x)
    val y_sorted = sortByScoringFunction(y)
    // param function extracts the Array[Double] part.
    val result = calculate(y_sorted.map(x => x._1))
    result
  }

  // The overall effect of the function is to transform an iterator of arrays into an iterator of tuples,
  // where each tuple contains the original array and a sum computed based on the logarithm of each element in the array.
  private def addScoringFunction(array: Iterator[Array[Double]]): Iterator[(Array[Double], Double)] = {
    array.map { x =>
      val sum = x.map(element => math.log(element + 1)).sum
      (x, sum)
    }
  }

  // takes an iterator of tuples, where each tuple contains an Array[Double] and a Double,
  // and sorts the tuples based on the second element of the tuple (the Double)
  private def sortByScoringFunction(iterator: Iterator[(Array[Double], Double)]): Iterator[(Array[Double], Double)] = {
    iterator.toArray.sortBy(x => -x._2).toIterator
  }

  def calculate(x: Iterator[Array[Double]]): Iterator[Array[Double]] = {
    val array = x.toArray
    val arrayBuffer = ArrayBuffer(array(0))

    for (i <- 1 until array.length) {
      var j = 0
      var shouldAdd = true

      while (j < arrayBuffer.length) {
        if (isDominated(array(i), arrayBuffer(j))) {
          arrayBuffer.remove(j)
          j -= 1
        } else if (isDominated(arrayBuffer(j), array(i))) {
          shouldAdd = false
        }
        j += 1
      }

      if (shouldAdd)
        arrayBuffer += array(i)
    }

    arrayBuffer.toIterator
  }

}
