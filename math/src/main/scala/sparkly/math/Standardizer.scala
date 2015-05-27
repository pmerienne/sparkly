package sparkly.math

case class VectorStandardizer(delegates: Array[Standardizer]) {
  def add(values: Array[Double]): (VectorStandardizer, Array[Double]) = {
    val updated = (this.delegates, values).zipped.map{case (standardizer, value) => standardizer.add(value)}
    (VectorStandardizer(updated.map(_._1)), updated.map(_._2))
  }

  def +(other: VectorStandardizer): VectorStandardizer = {
    val delegates = (this.delegates, other.delegates).zipped.map{case (d1, d2) => d1 + d2}
    VectorStandardizer(delegates)
  }

  def standardize(values: Array[Double]): Array[Double] = (delegates, values).zipped.map{case (d, v) => d.standardize(v)}
  def inverse(values: Array[Double]): Array[Double] = (delegates, values).zipped.map{case (d, v) => d.inverse(v)}
}

object VectorStandardizer {
  def apply(size: Int) = new VectorStandardizer(Array.fill(3)(Standardizer()))
}

case class Standardizer(n: Long = 0L, mu: Double = 0.0, mu2: Double = 0.0) {

  def add(value: Double): (Standardizer, Double) = {
    val n = this.n + 1
    val delta = value - this.mu
    val mu = this.mu + delta / n
    val mu2 = this.mu2 + delta * (value - mu)
    val updated = Standardizer(n, mu, mu2)
    (updated, updated.standardize(value))
  }

  def standardize(value: Double): Double = (value - mu) / stdev match {
    case x if(x.isNaN) => 0.0
    case x => x
  }
  def inverse(standardized: Double): Double = standardized * stdev + mu

  def stdev: Double = {
    if (n == 0) {
      Double.NaN
    } else {
      Math.sqrt(mu2 / n)
    }
  }

  def +(other: Standardizer): Standardizer = {
    if (this.n == 0) {
      other
    } else if (other.n == 0) {
      this
    } else {
      val delta = other.mu - this.mu
      val mu = if (other.n * 10 < this.n) {
        this.mu + (delta * other.n) / (this.n + other.n)
      } else if (this.n * 10 < other.n) {
        other.mu - (delta * this.n) / (this.n + other.n)
      } else {
        (this.mu * this.n + other.mu * other.n) / (this.n + other.n)
      }
      val mu2 = this.mu2 + other.mu2 + (delta * delta * this.n * other.n) / (this.n + other.n)
      val n = this.n + other.n
      Standardizer(n, mu, mu2)
    }
  }
}
