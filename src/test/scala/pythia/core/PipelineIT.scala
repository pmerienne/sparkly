package pythia.core

import java.nio.file.Files

import org.apache.spark.streaming._
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Span}
import org.scalatest.{FlatSpec, Matchers}
import pythia.component.classifier.Perceptron
import pythia.testing._
import pythia.component.source.CsvFileDirectorySource
import pythia.component.preprocess.Normalizer

class PipelineIT extends FlatSpec with Matchers with Eventually with SpamData {

  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(20, org.scalatest.time.Seconds)), interval = scaled(Span(100, Millis)))

  val pipelineBuilder = new PipelineBuilder()

  "Pipeline" should "build and connect components together" in {
    val pipelineConfig = PipelineConfiguration (
      name = "test",
      components = List (
        ComponentConfiguration (
          id = "csv_source",
          name = "Train data",
          clazz = classOf[CsvFileDirectorySource].getName,
          properties = Map (
            "Directory" -> "src/test/resources",
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
      )
    )

    // System init
    val ssc = new StreamingContext("local", "Test", Seconds(1))
    ssc.checkpoint(Files.createTempDirectory("pythia-test").toString)

    val availableStreams = pipelineBuilder.build(ssc, pipelineConfig)
    val accuracies = InspectedStream(availableStreams(("perceptron" ,"Accuracy")))

    try {
      ssc.start()
      eventually {
        accuracies.instances.last.rawFeatures("Accuracy").asInstanceOf[Double] should be > 0.80
      }
    } finally {
      ssc.stop()
    }

  }
}
