package pythia.component.preprocess

import pythia.core._
import pythia.testing.InspectedStream
import pythia.component.ComponentSpec

class NormalizerSpec extends ComponentSpec {

  "Normalizer" should "normalize features" in {
    // Given
    val inputStream = mockedStream()
    val configuration = ComponentConfiguration (
      clazz = classOf[Normalizer].getName,
      name = "Normalizer",
      inputs = Map (
        "Input" -> StreamConfiguration(selectedFeatures = Map("Features" -> List("f1", "f2", "f3")))
      )
    )

    // When
    val outputs: Map[String, InspectedStream] = deployComponent(configuration, Map("Input" -> inputStream.dstream))
    inputStream.push (
      Instance("f1" -> 0, "f2" -> 3, "f3" -> 4),
      Instance("f1" -> 6, "f2" -> 0, "f3" -> 8)
    )

    // Then
    eventually {
      outputs("Output").features should contain only (
        Map("f1" -> 0.0, "f2" -> 0.6, "f3" -> 0.8),
        Map("f1" -> 0.6, "f2" -> 0.0, "f3" -> 0.8)
      )
    }
  }
}
