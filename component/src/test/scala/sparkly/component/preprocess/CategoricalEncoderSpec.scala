package sparkly.component.preprocess

import sparkly.core._
import sparkly.testing._

class CategoricalEncoderSpec extends ComponentSpec {

  "CategoricalEncoder" should "encode categorical feature into double array" in {
    // Given
    val configuration = ComponentConfiguration(
      clazz = classOf[CategoricalEncoder].getName,
      name = "CategoricalEncoder",
      inputs = Map("Input" -> StreamConfiguration(mappedFeatures = Map("Feature" -> "country"))),
      outputs = Map("Output" -> StreamConfiguration(mappedFeatures = Map("Encoded feature" -> "country features"))),
      properties = Map("Distinct categories (n)" -> "5")
    )

    // When
    val component = deployComponent(configuration)
    component.inputs("Input").push(
      Instance("country" -> "ES"),
      Instance("country" -> "EN"),
      Instance("country" -> "IT"),
      Instance("country" -> "PT"),
      Instance("country" -> "PT"),
      Instance("country" -> "EN"),
      Instance("country" -> "FR")
    )

    // Then
    eventually {
      component.outputs("Output").features contains only (
        Map("country" -> "ES", "country features" -> Array(1.0, 0.0, 0.0, 0.0, 0.0)),
        Map("country" -> "EN", "country features" -> Array(0.0, 1.0, 0.0, 0.0, 0.0)),
        Map("country" -> "IT", "country features" -> Array(0.0, 0.0, 1.0, 0.0, 0.0)),
        Map("country" -> "PT", "country features" -> Array(0.0, 0.0, 0.0, 1.0, 0.0)),
        Map("country" -> "PT", "country features" -> Array(0.0, 0.0, 0.0, 1.0, 0.0)),
        Map("country" -> "EN", "country features" -> Array(0.0, 1.0, 0.0, 0.0, 0.0)),
        Map("country" -> "FR", "country features" -> Array(0.0, 0.0, 0.0, 0.0, 1.0))
      )
    }
  }

  "CategoricalEncoder" should "add 0 filled array when there's too much categories feature into double " in {
    // Given
    val configuration = ComponentConfiguration(
      clazz = classOf[CategoricalEncoder].getName,
      name = "CategoricalEncoder",
      inputs = Map("Input" -> StreamConfiguration(mappedFeatures = Map("Feature" -> "country"))),
      outputs = Map("Output" -> StreamConfiguration(mappedFeatures = Map("Encoded feature" -> "country features"))),
      properties = Map("Distinct categories (n)" -> "2")
    )

    // When
    val component = deployComponent(configuration)
    component.inputs("Input").push(
      Instance("country" -> "ES"),
      Instance("country" -> "EN"),
      Instance("country" -> "IT"),
      Instance("country" -> "ES"),
      Instance("country" -> "EN")
    )

    // Then
    eventually {
      component.outputs("Output").features contains only (
        Map("country" -> "ES", "country features" -> Array(1.0, 0.0)),
        Map("country" -> "EN", "country features" -> Array(0.0, 1.0)),
        Map("country" -> "IT", "country features" -> Array(0.0, 0.0)),
        Map("country" -> "ES", "country features" -> Array(1.0, 0.0)),
        Map("country" -> "EN", "country features" -> Array(0.0, 1.0))
      )
    }
  }
}

