package org.apache.spark.mllib.linalg

object VectorUtil {

  implicit class VectorPublications(val vector : org.apache.spark.mllib.linalg.Vector) extends AnyVal {
    def toBreeze : breeze.linalg.Vector[scala.Double] = vector.toBreeze

  }

  implicit class BreezeVectorPublications(val breezeVector : breeze.linalg.Vector[Double]) extends AnyVal {
    def toSpark : org.apache.spark.mllib.linalg.Vector = Vectors.fromBreeze(breezeVector)

    def toDenseSpark : org.apache.spark.mllib.linalg.Vector = breezeVector match {
      case v: breeze.linalg.DenseVector[Double] => v.toSpark
      case _ => breezeVector.toDenseVector.toSpark
    }

    def toDenseBreeze: breeze.linalg.DenseVector[Double] = breezeVector match {
      case v: breeze.linalg.DenseVector[Double] => v
      case _ => breezeVector.toDenseVector
    }
  }
}