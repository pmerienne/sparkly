package pythia.component.preprocess

import pythia.testing._
import pythia.core._

class NormalizerSpec extends ComponentSpec {

  "Normalizer" should "normalize features" in {
    // Given
    val configuration = ComponentConfiguration (
      clazz = classOf[Normalizer].getName,
      name = "Normalizer",
      inputs = Map (
        "Input" -> StreamConfiguration(selectedFeatures = Map("Features" -> List("f1", "f2", "f3")))
      )
    )

    // When
    val component = deployComponent(configuration)
    component.inputs("Input").push (
      Instance("f1" -> 0, "f2" -> 3, "f3" -> 4),
      Instance("f1" -> 6, "f2" -> 0, "f3" -> 8)
    )

    // Then
    eventually {
      component.outputs("Output").features should contain only (
        Map("f1" -> 0.0, "f2" -> 0.6, "f3" -> 0.8),
        Map("f1" -> 0.6, "f2" -> 0.0, "f3" -> 0.8)
      )
    }
  }
}
