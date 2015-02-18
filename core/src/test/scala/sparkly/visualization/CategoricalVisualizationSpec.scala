package sparkly.visualization

import sparkly.core._

class CategoricalVisualizationSpec extends VisualizationSpec {

  "CategoricalVisualization" should "send distinct feature's count" in {
    // Given
    val configuration = VisualizationConfiguration (
      name = "Instance rate", clazz = classOf[CategoricalVisualization].getName,
      properties = Map("Window length (in ms)" -> "1000"),
      features = Map("Categorical feature (String, Boolean)" -> FeatureIdentifier("component", "stream", "country"))
    )

    // When
    val build = deployVisualization(configuration)

    val data = List("FR", "EN", "ES", "EN", null, "EN", null, "ES").map(country => Instance("country" -> country)).toList
    build.mockStream("component", "stream", "country").push(data)

    // Then
    eventually {
      val data = latestSentOutData()
      data should contain only (
        "EN" -> 3.0, "ES" -> 2.0, "FR" -> 1.0, "$MISSING_FEATURE$" -> 2.0, "$TOTAL$" -> 8
      )
    }
  }

  "CategoricalVisualization" should "limit distinct features" in {
    // Given
    val configuration = VisualizationConfiguration (
      name = "Category distribution", clazz = classOf[CategoricalVisualization].getName,
      properties = Map("Window length (in ms)" -> "1000", "Max category (0 for unlimited)" -> "2"),
      features = Map("Categorical feature (String, Boolean)" -> FeatureIdentifier("component", "stream", "country"))
    )

    // When
    val build = deployVisualization(configuration)

    val data = List("FR", "EN", "ES", "EN", null, "EN", null, "ES").map(country => Instance("country" -> country)).toList
    build.mockStream("component", "stream", "country").push(data)

    // Then
    eventually {
      val data = latestSentOutData()
      data should contain only (
        "EN" -> 3.0, "ES" -> 2.0, "$TOTAL$" -> 8
      )
    }
  }

}
