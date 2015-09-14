package sparkly.component.nlp

import breeze.linalg._
import breeze.numerics._

import scala.collection.mutable

object TfIdfModel {
  def apply(vocabularySize: Int): TfIdfModel = {
    TfIdfModel(vocabularySize, 0, 0.0, SparseVector.zeros[Double](vocabularySize))
  }

  def apply(vocabularySize: Int, minDocFreq: Double): TfIdfModel = {
    TfIdfModel(vocabularySize, minDocFreq, 0.0, SparseVector.zeros[Double](vocabularySize))
  }
}

case class TfIdfModel(vocabularySize: Int, minDocFreq: Double, documentCount: Double, termsFrequencies: SparseVector[Double]) {

  lazy val idf = if(minDocFreq > 0.0) {
    val minFrequency = documentCount * minDocFreq
    termsFrequencies.mapActiveValues{ f => if(f < minFrequency) 0.0 else Math.log(documentCount / f)}
  } else {
    termsFrequencies.mapActiveValues{ f => Math.log(documentCount / f)}
  }

  def add(terms: List[String]): TfIdfModel = {
    val termsCount = mutable.HashMap.empty[Int, Double]
    terms.foreach { term =>
      val i = indexOf(term)
      termsCount.put(i, 1.0)
    }

    val (indices, values) = termsCount.toSeq.sortBy(_._1).unzip
    val termsInverseFrequencies = new SparseVector[Double](indices.toArray, values.toArray, vocabularySize)
    this.copy(documentCount = documentCount + 1.0, termsFrequencies = termsFrequencies :+ termsInverseFrequencies)
  }

  def +(other: TfIdfModel): TfIdfModel = {
    this.copy(documentCount = this.documentCount + other.documentCount, termsFrequencies = this.termsFrequencies + other.termsFrequencies)
  }

  def tfIdf(terms: List[String]): SparseVector[Double] = {
    tf(terms) :* idf
  }

  def tf(terms: List[String]): SparseVector[Double] = {
    val incr = 1.0 / terms.size.toDouble

    val frequencies = mutable.HashMap.empty[Int, Double]
    terms.foreach { term =>
      val i = indexOf(term)
      frequencies.put(i, frequencies.getOrElse(i, 0.0) + incr)
    }

    val (indices, values) = frequencies.toSeq.sortBy(_._1).unzip
    new SparseVector[Double](indices.toArray, values.toArray, vocabularySize)
  }

  private def indexOf(term: String): Int = {
    val rawMod = term.## % vocabularySize
    rawMod + (if (rawMod < 0) vocabularySize else 0)
  }
}
