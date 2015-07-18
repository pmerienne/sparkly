package sparkly.component.regressor

import org.apache.spark.rdd.RDD

case class RunningRmsd(sum: Double = 0.0, count: Long = 0, min: Double = Double.MaxValue, max: Double = Double.MinValue) {

  def update(actual: Double, expected: Double): RunningRmsd = {
    this.copy(sum = sum + Math.pow(actual - expected, 2), count = count + 1, min = Math.min(min, expected), max = Math.max(max, expected))
  }

  def update(actual: RDD[Double], expected: RDD[Double]): RunningRmsd = {
    this + expected
      .zip(actual)
      .map{case (e, a) => RunningRmsd(Math.pow(a - e, 2), 1L, e, e)}
      .reduce(_ + _)
  }

  def +(other: RunningRmsd): RunningRmsd = {
    RunningRmsd(this.sum + other.sum, this.count + other.count, Math.min(this.min, other.min), Math.max(this.max, other.max))
  }

  def value = Math.sqrt(sum / count)
  def normalized = value / (max - min)
}

