package sparkly.it

import java.io.File
import java.nio.file.Files

import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Span}
import org.scalatest.{FlatSpec, Matchers}
import sparkly.component.classifier.Perceptron
import sparkly.component.preprocess.Normalizer
import sparkly.component.source.CsvFileDirectorySource
import sparkly.core._
import sparkly.testing._

import scala.reflect.io.Directory

class PipelineIT extends FlatSpec with Matchers with Eventually with SpamData {

  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(20, org.scalatest.time.Seconds)), interval = scaled(Span(100, Millis)))

  val pipelineDirectory = Directory.makeTemp()
  val streamingContextFactory = new StreamingContextFactory(pipelineDirectory.toString, "local[8]", "test-cluster", "localhost", 8080)
  val workingDirectory = Directory.makeTemp()

  "Pipeline" should "build and connect components together" in {
    val pipelineConfig = PipelineConfiguration (
      name = "test",
      components = List (
        ComponentConfiguration (
          id = "csv_source",
          name = "Train data",
          clazz = classOf[CsvFileDirectorySource].getName,
          properties = Map (
            "Directory" -> workingDirectory.toString,
            "Process only new files" -> "false",
            "Filename pattern" -> "spam.data"
          ),
          outputs = Map (
            "Instances" -> StreamConfiguration(selectedFeatures = Map("Features" -> (labelName::featureNames)))
          )
        ),
        ComponentConfiguration (
          id = "normalizer",
          name = "Normalizer",
          clazz = classOf[Normalizer].getName,
          inputs = Map (
            "Input" -> StreamConfiguration(selectedFeatures = Map("Features" -> featureNames))
          )
        ),
        ComponentConfiguration (
          id = "perceptron",
          name = "Learner",
          clazz = classOf[Perceptron].getName,
          properties = Map (
            "Bias" -> "1.0",
            "Learning rate" -> "1.0"
          ),
          inputs = Map (
            "Train" -> StreamConfiguration(mappedFeatures = Map("Label" -> labelName), selectedFeatures = Map("Features" -> featureNames)),
            "Prediction query" -> StreamConfiguration(selectedFeatures = Map("Features" -> featureNames))
          ),
          outputs = Map (
            "Prediction result" -> StreamConfiguration(mappedFeatures = Map("Label" -> "prediction")),
            "Accuracy" -> StreamConfiguration(mappedFeatures = Map("Accuracy" -> "Accuracy"))
          )
        )
      ),
      connections = List (
        ConnectionConfiguration("csv_source", "Instances", "normalizer", "Input"),
        ConnectionConfiguration("normalizer", "Output", "perceptron", "Train")
      ),
      batchDurationMs = 200
    )

    // System init
    val (ssc, build) = streamingContextFactory.createStreamingContext(pipelineConfig)
    val accuracies = InspectedStream(build.outputs(("perceptron" ,"Accuracy")))

    try {
      ssc.start()
      Files.copy(getClass.getResourceAsStream("/spam.data"), new File(workingDirectory.toString, "spam.data").toPath())
      eventually {
        accuracies.instances.last.rawFeatures("Accuracy").asInstanceOf[Double] should be > 0.80
      }
    } finally {
      ssc.stop()
    }

  }
}
