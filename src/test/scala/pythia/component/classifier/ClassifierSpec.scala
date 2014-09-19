package pythia.component.classifier

import org.scalatest.BeforeAndAfterEach
import pythia.component.ComponentSpec
import pythia.core._
import pythia.testing._

import scala.util.Random

trait ClassifierSpec extends ComponentSpec with BeforeAndAfterEach {
  def dataset: List[Instance]
  def split(dataset: List[Instance], percent: Double = 0.80) = Random.shuffle(dataset).partition(i => Random.nextDouble() < 0.80)

  def featureNames: List[String]
  def labelName: String

  val (trainDataset, testDataset) = split(dataset)

  var trainStream: MockStream = _
  var predictionQueryStream: MockStream = _

  var outputs: Map[String, InspectedStream] = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    trainStream = mockedStream()
    predictionQueryStream = mockedStream()
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
    val inputs = Map("Train" -> trainStream.dstream, "Prediction query" -> predictionQueryStream.dstream)
    outputs = deployComponent(configuration, inputs)
  }

  def train(classierClass: Class[_ <: Component]) = {
    initClassifier(classOf[Perceptron])
    trainStream.push(trainDataset)

    val accuracies = outputs("Accuracy").instances
    eventually {
      accuracies should have size trainDataset.size
    }

    val trainingError = accuracies.last.rawFeatures("Accuracy")
    println(s"Training accuracy : ${trainingError}")
  }

  def test(): Double = {
    predictionQueryStream.push(testDataset)
    val results = outputs("Prediction result").instances

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

