package sparkly.component.classifier

import sparkly.testing._
import sparkly.core._

import scala.io.Source

class LogisticBinaryClassifierWithSGDSpec extends ComponentSpec {

  "LogisticBinaryClassifierWithSGD" should "train on spam data" in {
    // Given
    val configuration = ComponentConfiguration (
      name = "LogisticBinaryClassifierWithSGD",
      clazz = classOf[LogisticBinaryClassifierWithSGD].getName,
      inputs = Map(
        "Train" -> StreamConfiguration(mappedFeatures = Map("Label" -> "f0"), selectedFeatures = Map("Features" -> List.range(1, 56).map(index => s"f${index}"))),
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

    val training = Source.fromInputStream(getClass.getResourceAsStream("/dataset/spam.data")).getLines().map{ line =>
      val fields = line.split(";").zipWithIndex
      val values = fields.map{case (value, index) => s"f$index" -> value.toDouble}.toMap
      Instance(values)
    }
    component.inputs("Train").push(2000, training)

    // Then
    eventually {
      val monitoringData = component.latestMonitoringData[Map[String, Double]]("Accuracy")
      val accuracy = monitoringData.data("Accuracy")
      accuracy should be > 80.0
    }
  }


}
