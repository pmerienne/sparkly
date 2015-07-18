package sparkly.component.regressor

import scala.Some
import scala.util.Random
import sparkly.core._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.VectorUtil._

class LinearRegressorWithSGD extends Component {

  override def metadata: ComponentMetadata = ComponentMetadata (
    name = "Linear regressor", category = "Regressor",
    description =
      """
        |Regressor based on MLlib's linear regression model with SGD.
        |
      """.stripMargin,
    inputs = Map (
      "Train" -> InputStreamMetadata(namedFeatures = Map("Label" -> FeatureType.CONTINUOUS, "Features" -> FeatureType.VECTOR)),
      "Predict" -> InputStreamMetadata(namedFeatures = Map("Features" -> FeatureType.VECTOR))
    ),outputs = Map (
      "Predictions" -> OutputStreamMetadata(from = Some("Predict"), namedFeatures = Map("Prediction" -> FeatureType.CONTINUOUS))
    ), properties = Map (
      "Step size" -> PropertyMetadata(PropertyType.DECIMAL, defaultValue = Some(0.1), description = "Step size for gradient descent"),
      "Iterations" -> PropertyMetadata(PropertyType.INTEGER, defaultValue = Some(2), description = "Number of iterations of gradient descent to run per batch"),
      "Batch fraction" -> PropertyMetadata(PropertyType.DECIMAL, defaultValue = Some(1.0), description = "Fraction of each batch to use for updates")
    ),
    monitorings = Map("NRMSD" -> MonitoringMetadata(ChartType.LINES, values = List("NRMSD"), primaryValues = List("RMSD", "NRMSD")))
  )

  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val stepSize = context.properties("Step size").as[Double]
    val iterations = context.properties("Iterations").as[Int]
    val miniBatchFraction = context.properties("Batch fraction").as[Double]

    val model = new SparklyStreamingLinearRegressorWithSGD(context.createMonitoring[Map[String, Double]]("NRMSD"))
      .setStepSize(stepSize)
      .setNumIterations(iterations)
      .setMiniBatchFraction(miniBatchFraction)

    val train = context.dstream("Train").map{ i =>
      val label = i.inputFeature("Label").asDouble
      val features = i.inputFeature("Features").asVector.toSpark
      LabeledPoint(label, features)
    }

    model.testThenTrainOn(train)

    val predictions = context
      .dstream("Predict", "Predictions").map{ i =>
      val features = i.inputFeature("Features").asVector.toSpark
      val label = model.predict(features)
      i.outputFeature("Prediction", label)
    }

    Map("Predictions" -> predictions)
  }

}


class SparklyStreamingLinearRegressorWithSGD(monitoring: Monitoring[Map[String, Double]]) extends StreamingLinearRegressionWithSGD {

  var rmsd: RunningRmsd = RunningRmsd()

  def testThenTrainOn(data: DStream[LabeledPoint]): Unit = {
    data.foreachRDD { (rdd, time) => if (!rdd.isEmpty) {
      rdd.cache()

      // Test
      model match {
        case Some(m) => {
          val actual = m.predict(rdd.map(_.features))
          val expected = rdd.map(_.label)
          rmsd = rmsd.update(actual, expected)
          monitoring.add(time.milliseconds, Map("RMSD" -> rmsd.value, "NRMSD" -> rmsd.normalized))
        }
        case None => // Do nothing
      }

      // Train
      val weights = model match {
        case Some(m) => m.weights
        case None => initWeights(rdd.first.features)
      }
      model = Some(algorithm.run(rdd, weights))
    }}
  }

  def predict(features: Vector): Double = model match {
    case Some(m) => m.predict(features)
    case None => 0.0
  }

  private def initWeights(features: Vector): Vector = {
    val size = features.size
    Vectors.dense(Array.fill(size)(Random.nextDouble))
  }
}