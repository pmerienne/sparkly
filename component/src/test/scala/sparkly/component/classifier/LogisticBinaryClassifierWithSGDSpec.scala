package sparkly.component.classifier

import sparkly.testing._
import sparkly.core._

import scala.io.Source
import sparkly.component.source.dataset.SpamDataset

class LogisticBinaryClassifierWithSGDSpec extends ComponentSpec {

  "LogisticBinaryClassifierWithSGD" should "train on spam data" in {
    // Given
    val configuration = ComponentConfiguration (
      name = "LogisticBinaryClassifierWithSGD",
      clazz = classOf[LogisticBinaryClassifierWithSGD].getName,
      inputs = Map(
        "Train" -> StreamConfiguration(mappedFeatures = Map("Label" -> SpamDataset.labelName), selectedFeatures = Map("Features" -> SpamDataset.featureNames)),
        "Predict" -> StreamConfiguration(selectedFeatures = Map("Features" -> List.range(1, 56).map(index => s"f${index}")))
      ),
      outputs = Map("Predictions" -> StreamConfiguration(mappedFeatures = Map("Label" -> "prediction"))),
      properties = Map (
        "Iterations" -> "2"
      ),
      monitorings = Map("Accuracy" -> MonitoringConfiguration(active = true))
    )

    // When
    val component = deployComponent(configuration)
    component.inputs("Train").push(2000, SpamDataset.iterator())

    // Then
    eventually {
      val monitoringData = component.latestMonitoringData[Map[String, Double]]("Accuracy")
      val accuracy = monitoringData.data("Accuracy")
      accuracy should be > 80.0
    }
  }


}
