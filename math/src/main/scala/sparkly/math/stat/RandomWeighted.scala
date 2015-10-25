package sparkly.math.stat

import scala.util.Random

case class RandomWeighted[T](values: Seq[(T, Double)]) {

  private val cummulativeWeights = values.scanLeft(0.0)(_ + _._2).tail
  private val sum = cummulativeWeights.last

  def randomElement(): T = {
    val limit = sum * Random.nextDouble()
    val index = cummulativeWeights.indexWhere(_ >= limit)
    values(index)._1
  }

}
