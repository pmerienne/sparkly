package sparkly.math

import breeze.linalg._
import breeze.numerics._

object Standardizer {
  def apply(size: Int): Standardizer = {
    Standardizer(size, 0, DenseVector.fill(size, 0.0), DenseVector.fill(size, 0.0))
  }

  def apply(size: Int, data: Iterator[DenseVector[Double]]): Standardizer = {
    var standardizer = Standardizer(size, 0, DenseVector.fill(size, 0.0), DenseVector.fill(size, 0.0))
    data.foreach { vector =>
      standardizer = standardizer.update(vector)
    }
    standardizer
  }
}

case class Standardizer(size: Int, n: Long, mu: DenseVector[Double], mu2: DenseVector[Double]) {

  def standardize(value: DenseVector[Double]): DenseVector[Double] = (value - mu) / stddev
  def inverse(standardized: DenseVector[Double]): DenseVector[Double] = (standardized :* stddev) + mu

  def stddev(): DenseVector[Double] = {
    if (n == 0) {
      DenseVector.fill(size, Double.PositiveInfinity)
    } else {
      sqrt(mu2 :/ n.toDouble)
    }
  }

  def update(values: DenseVector[Double]): Standardizer = {
    val newN = this.n + 1
    val newDelta = values - this.mu
    val newMu = this.mu + (newDelta :/ newN.toDouble)
    val newMu2 = this.mu2 + (newDelta :* (values - newMu))
    this.copy(n = newN, mu = newMu, mu2 = newMu2)
  }

  def +(other: Standardizer): Standardizer = {
    if (this.n == 0) {
      other
    } else if (other.n == 0) {
      this
    } else {
      val delta = other.mu - this.mu
      val mu = if (other.n * 10 < this.n) {
        this.mu + ((delta * other.n.toDouble) / (this.n + other.n).toDouble)
      } else if (this.n * 10 < other.n) {
        other.mu - (delta * this.n.toDouble) / (this.n.toDouble + other.n.toDouble)
      } else {
        ((this.mu * this.n.toDouble) + (other.mu * other.n.toDouble)) :/ (this.n + other.n).toDouble
      }
      val mu2 = this.mu2 + other.mu2 + (((delta :* delta) * (this.n * other.n).toDouble) / (this.n + other.n).toDouble)
      val n = this.n + other.n
      Standardizer(this.size, n, mu, mu2)
    }
  }
}
