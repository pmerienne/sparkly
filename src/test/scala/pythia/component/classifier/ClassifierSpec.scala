package pythia.component.classifier

import org.scalatest.BeforeAndAfterEach
import pythia.component.{ComponentSpec, RunningComponent}
import pythia.core._

import scala.util.Random

trait ClassifierSpec extends ComponentSpec with BeforeAndAfterEach {
  def dataset: List[Instance]
  def split(dataset: List[Instance], percent: Double = 0.80) = Random.shuffle(dataset).partition(i => Random.nextDouble() < 0.80)

  def featureNames: List[String]
  def labelName: String

  val (trainDataset, testDataset) = split(dataset)

  var runningComponent: RunningComponent = _

  override def beforeEach(): Unit = {
    super.beforeEach()
  }

  def initClassifier(classierClass: Class[_ <: Component]) = {
    val configuration = ComponentConfiguration (
      clazz = classierClass.getName,
      name = "Classifier",
      inputs = Map (
        "Train" -> StreamConfiguration(mappedFeatures = Map("Label" -> labelName), selectedFeatures = Map("Features" -> featureNames)),
        "Prediction query" -> StreamConfiguration(selectedFeatures = Map("Features" -> featureNames))
      ),
      outputs = Map (
        "Prediction result" -> StreamConfiguration(mappedFeatures = Map("Label" -> "Label")),
        "Accuracy" -> StreamConfiguration(mappedFeatures = Map("Name" -> "Name", "Accuracy" -> "Accuracy"))
      )
    )

    runningComponent = deployComponent(configuration)
  }

  def train(classierClass: Class[_ <: Component]) = {
    initClassifier(classOf[Perceptron])
    runningComponent.inputs("Train").push(trainDataset)

    val accuracies = runningComponent.outputs("Accuracy").instances
    eventually {
      accuracies should have size trainDataset.size
    }

    val trainingError = accuracies.last.rawFeatures("Accuracy")
    println(s"Training accuracy : ${trainingError}")
  }

  def test(): Double = {
    runningComponent.inputs("Prediction query").push(testDataset)
    val results = runningComponent.outputs("Prediction result").instances

    eventually {
      results should have size testDataset.size
    }

    val errorCount = results
      .map{ instance => (instance.rawFeatures(labelName) != "0", instance.outputFeature("Label").as[Boolean])}
      .filter{ case (expected, actual) => expected != actual}
      .size

    val validationError = errorCount.toDouble / results.size.toDouble
    println(s"Validation error : ${validationError}")

    validationError
  }

  def eval(classierClass: Class[_ <: Component]) = {
    train(classierClass)
    test()
  }
}

