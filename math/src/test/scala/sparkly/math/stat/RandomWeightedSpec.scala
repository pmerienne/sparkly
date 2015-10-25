package sparkly.math.stat

import org.scalatest._

class RandomWeightedSpec extends FlatSpec with Matchers {

  "RandomWeighted" should "get random element" in {
    // Given
    val data = List("a" -> 0.5, "b" -> 0.10, "c" -> 0.10, "d" -> 0.30)

    // When
    val randomWeigthed = RandomWeighted(data)

    // Then
    val distributions = List
      .fill(1000000)(randomWeigthed.randomElement())
      .groupBy(_.toString)
      .mapValues(_.size / 1000000.0)
      .toMap

    distributions("a") should be (0.5 +- 0.01)
    distributions("b") should be (0.1 +- 0.01)
    distributions("c") should be (0.1 +- 0.01)
    distributions("d") should be (0.3 +- 0.01)
  }

}
