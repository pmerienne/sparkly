package sparkly.math

import org.scalatest._
import scala.util.Random

class StandardizerSpec extends FlatSpec with Matchers {

  "Standardizer" should "standardize values" in {
    // Given
    val originalValues = (1 to 1000).map(i => 10.0 + Random.nextGaussian).toList

    // When
    val (standardizedValues, standardizer) = originalValues.foldLeft((List[Double](), Standardizer())){(d, value) =>
      val (updated, standardized) = d._2 add value
      (d._1 :+ standardized, updated)
    }
    val destandardizedValues = standardizedValues.map(v => standardizer.inverse(v))

    // Then
    stddev(standardizedValues) should be (1.0 +- 0.1)
    rmse(originalValues, destandardizedValues) < 0.2
  }

  def rmse(x: List[Double], y: List[Double]): Double = {
    val squares = (x zip y).map{case(x, y) => Math.pow(x - y, 2)}
    Math.sqrt(squares.sum / squares.size)
  }

  def stddev(values: List[Double]): Double = {
    val mean = values.sum / values.size
    val devs = values.map(v => (v - mean) * (v - mean))
    Math.sqrt(devs.sum / values.size)
  }
}
