object BasicSkyline extends Serializable {
  def calculate(x: Iterator[Array[Double]]): Iterator[Array[Double]] = {
    val data_points = x.toList

    val nonDominatedPoints = data_points.filter { point =>
      data_points.forall(otherPoint => !isDominated(otherPoint, point))
    }

    nonDominatedPoints.toIterator
  }

  // This function checks if y is dominated by x
  // zip function pairs corresponding elements of arrays x and y.
  def isDominated(x: Array[Double], y: Array[Double]): Boolean = {
    x.zip(y).exists {
      case (xi, yi) => xi < yi // check if there exists at least one pair where xi is strictly less than yi (for strict dominance).
    } && !x.zip(y).exists {
      case (xi, yi) => xi > yi // check if there is no pair where xi is strictly greater than yi (for weak dominance).
    }
  }
}
