package nl.trivento.fastdata.ndw.processor

/**
  * Created by koen on 26/05/2017.
  */
object Trend {
  def apply(data: Array[Double]): Trend = {
    // https://www.quora.com/Simple-algorithm-for-trend-detection-in-time-series-data
    val total = data.sum
    val N = data.length
    val weighed: Double = data.zipWithIndex.map(t => t._1 * (t._2 + 1)).sum

    val start = (2 * (2 * N + 1) * total - 6 * weighed) / (N * (N - 1))
    val increment = ((12 * weighed) - 6 * (N + 1) * total) / (N * (N - 1) * (N + 1))
    Trend(start, increment)
  }

  def main(args: Array[String]): Unit = {
    val trend = Trend(Array(1, 2.2, 3, 4.1, 4.9, 6.05))
    System.out.println(Range(0, 10).map(trend(_)).mkString(", "))
  }
}

case class Trend(start: Double, increment: Double) {
  def apply(t: Int): Double = start + increment * t
}
