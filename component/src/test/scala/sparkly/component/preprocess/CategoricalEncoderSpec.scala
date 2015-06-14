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
      outputs = Map("Output" -> StreamConfiguration(selectedFeatures = Map("Encoded features" -> List("country_1", "country_2", "country_3", "country_4", "country_5"))))
    )

    // When
    val component = deployComponent(configuration)
    component.inputs("Input").push(
      Instance("user" -> "user1", "country" -> "ES"),
      Instance("user" -> "user2", "country" -> "EN"),
      Instance("user" -> "user3", "country" -> "IT"),
      Instance("user" -> "user4", "country" -> "PT"),
      Instance("user" -> "user5", "country" -> "PT"),
      Instance("user" -> "user6", "country" -> "EN"),
      Instance("user" -> "user7", "country" -> "FR")
    )

    // Then
    eventually {
      component.outputs("Output").features contains only (
        Map("user" -> "user1", "country" -> "ES", "country_1" -> 1.0, "country_2" -> 0.0, "country_3" -> 0.0, "country_4" -> 0.0, "country_5" -> 0.0),
        Map("user" -> "user2", "country" -> "EN", "country_1" -> 0.0, "country_2" -> 1.0, "country_3" -> 0.0, "country_4" -> 0.0, "country_5" -> 0.0),
        Map("user" -> "user3", "country" -> "IT", "country_1" -> 0.0, "country_2" -> 0.0, "country_3" -> 1.0, "country_4" -> 0.0, "country_5" -> 0.0),
        Map("user" -> "user4", "country" -> "PT", "country_1" -> 0.0, "country_2" -> 0.0, "country_3" -> 0.0, "country_4" -> 1.0, "country_5" -> 0.0),
        Map("user" -> "user5", "country" -> "PT", "country_1" -> 0.0, "country_2" -> 0.0, "country_3" -> 0.0, "country_4" -> 1.0, "country_5" -> 0.0),
        Map("user" -> "user6", "country" -> "EN", "country_1" -> 0.0, "country_2" -> 1.0, "country_3" -> 0.0, "country_4" -> 0.0, "country_5" -> 0.0),
        Map("user" -> "user7", "country" -> "FR", "country_1" -> 0.0, "country_2" -> 0.0, "country_3" -> 0.0, "country_4" -> 0.0, "country_5" -> 1.0)
      )
    }
  }

  "CategoricalEncoder" should "add 0 filled array when there's too much categories feature into double " in {
    // Given
    val configuration = ComponentConfiguration(
      clazz = classOf[CategoricalEncoder].getName,
      name = "CategoricalEncoder",
      inputs = Map("Input" -> StreamConfiguration(mappedFeatures = Map("Feature" -> "country"))),
      outputs = Map("Output" -> StreamConfiguration(selectedFeatures = Map("Encoded features" -> List("country_1", "country_2"))))
    )

    // When
    val component = deployComponent(configuration)
    component.inputs("Input").push(
      Instance("user" -> "user1", "country" -> "ES"),
      Instance("user" -> "user2", "country" -> "EN"),
      Instance("user" -> "user3", "country" -> "IT"),
      Instance("user" -> "user4", "country" -> "ES"),
      Instance("user" -> "user5", "country" -> "EN")
    )

    // Then
    eventually {
      component.outputs("Output").features contains only (
        Map("user" -> "user1", "country" -> "ES", "country_1" -> 1.0, "country_2" -> 0.0),
        Map("user" -> "user2", "country" -> "EN", "country_1" -> 0.0, "country_2" -> 1.0),
        Map("user" -> "user3", "country" -> "IT", "country_1" -> 0.0, "country_2" -> 0.0),
        Map("user" -> "user4", "country" -> "ES", "country_1" -> 1.0, "country_2" -> 0.0),
        Map("user" -> "user5", "country" -> "EN", "country_1" -> 0.0, "country_2" -> 1.0)
      )
    }
  }
}

