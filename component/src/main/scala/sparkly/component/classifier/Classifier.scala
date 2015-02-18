package sparkly.component.classifier

import breeze.linalg.DenseVector
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import sparkly.core.FeatureType.{DOUBLE, STRING, NUMBER, ANY}
import sparkly.core._

import scala.reflect._
abstract class ClassifierComponent[L : ClassTag] extends Component {

  def metadata = ComponentMetadata("Classifier",
    inputs = Map (
      "Train" -> InputStreamMetadata(namedFeatures = Map("Label" -> ANY), listedFeatures = Map("Features" -> NUMBER)),
      "Prediction query" -> InputStreamMetadata(listedFeatures = Map("Features" -> NUMBER))
    ),
    outputs = Map (
      "Prediction result" -> OutputStreamMetadata(from = Some("Prediction query"), namedFeatures = Map("Label" -> ANY)),
      "Accuracy" -> OutputStreamMetadata(namedFeatures = Map("Name" -> STRING, "Accuracy" -> DOUBLE))
    )
  )

  def initModel(context: Context): ClassifierModel[L]
  def modelName(context: Context) = this.getClass.getSimpleName

  override def initStreams(context: Context):Map[String, DStream[Instance]] = {
    val accuracyMapper = context.outputMappers("Accuracy")

    val initialModel = (initModel(context), Accuracy(0, 0))
    val name = modelName(context)

    val train = context.dstream("Train")
      .map(instance => (name, instance))

    val query = context.dstream("Prediction query", "Prediction result")
      .map(instance => (name, instance))

    val updatedModels = train.updateStateByKey(updateModel(initialModel))

    val results = query
      .join(updatedModels)
      .map{case(name, (instance, state)) =>
        val model = state._1
        val features = instance.inputFeatures("Features").asDenseVector
        val prediction = model.classify(features)

        instance.outputFeature("Label", prediction)
      }

    val accuracies = train
      .join(updatedModels)
      .map{case(name, (instance, state)) =>
        val accuracy = state._2
        Instance(accuracyMapper.featureName("Accuracy") -> accuracy.value, accuracyMapper.featureName("Name") -> name)
      }

    Map("Prediction result" -> results, "Accuracy" -> accuracies)
  }

  def updateModel[L : ClassTag](initialModel: (ClassifierModel[L], Accuracy))(values: Seq[Instance], state: Option[(ClassifierModel[L], Accuracy)]): Option[(ClassifierModel[L], Accuracy)] = {
    val previous = state.getOrElse(initialModel)
    var previousModel = previous._1;
    var previousAccuracy = previous._2;

    values.foreach(instance => {
      val label = instance.inputFeature("Label").as[L]
      val features = instance.inputFeatures("Features").asDenseVector;
      val prediction = previousModel.classify(features)

      previousModel = previousModel.update(label, prediction, features)
      previousAccuracy = previousAccuracy.update(label, prediction)
    })

    Some((previousModel, previousAccuracy))
  }

}

abstract class ClassifierModel[L] extends Serializable {
  def classify(features: DenseVector[Double]): L
  def update(expected: L, prediction: L, features: DenseVector[Double]): ClassifierModel[L]

}

case class Accuracy(successCount: Int, total: Int) {
  def update(expected: Any, prediction: Any): Accuracy = {
    val success = if(expected == prediction) 1 else 0
    Accuracy(successCount + success, total + 1)
  }

  def value = successCount.toDouble / total.toDouble
}
