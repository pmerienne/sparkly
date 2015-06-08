package sparkly.component.classifier

import breeze.linalg.{DenseVector, Vector}
import sparkly.core._

import scala.Predef._

import sparkly.core.PropertyType._


class Perceptron extends ClassifierComponent {

  override def metadata = super.metadata.copy(name = "Perceptron classifier", category = "Classifier",
    properties = Map (
      "Name" -> PropertyMetadata(STRING, defaultValue = Some("Perceptron")),
      "Bias" -> PropertyMetadata(DECIMAL, defaultValue = Some(0.1)),
      "Learning rate" -> PropertyMetadata(DECIMAL, defaultValue = Some(0.1))
    )
  )

  override def initModel(context: Context): OneClassClassifierModel = {
    val bias = context.properties("Bias").as[Double]
    val learningRate = context.properties("Learning rate").as[Double]
    val featureCount = context.inputSize("Train", "Features")

    PerceptronModel(bias, learningRate, DenseVector.rand[Double](featureCount) .* (1.0 / featureCount))
  }

  override def modelName(context: Context) = context.properties("Name").as[String]
}

case class PerceptronModel(bias: Double, learningRate: Double, weights: Vector[Double]) extends OneClassClassifierModel {

  override def classify(features: DenseVector[Double]): Boolean = {
    features.t * weights + bias > 0
  }

  def update(expected: Boolean, prediction: Boolean, features: DenseVector[Double]): PerceptronModel = {
    val newWeights = if(prediction != expected) {
      val correction = learningRate * (if (expected) 1.0 else -1.0)
      weights + (features :* correction)
    } else {
      weights
    }

    PerceptronModel(bias, learningRate, newWeights)
  }

}
