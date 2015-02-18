package sparkly.visualization

import sparkly.core._

class FeatureStatisticsVisualizationSpec extends VisualizationSpec {

  "FeatureStatisticsVisualization" should "send distinct feature's statistics" in {
    // Given
    val configuration = VisualizationConfiguration (
      name = "Feature statistics", clazz = classOf[FeatureStatisticsVisualization].getName,
      properties = Map("Window length (in ms)" -> "1000"),
      features = Map("Number feature" -> FeatureIdentifier("component", "stream", "value"))
    )

    // When
    val build = deployVisualization(configuration)

    val data = List(-10, 15, null, -5, 20, 25, -10, 15, 10, null).map(value => Instance("value" -> value)).toList
    build.mockStream("component", "stream", "value").push(data)

    // Then
    eventually {
      val data = latestSentOutData()
      data("count") should equal (10.0)
      data("missing") should equal (2.0)
      data("min") should equal (-10.0)
      data("max") should equal (25.0)
      data("mean") should equal (7.5 +- 2)
      data("std") should equal (12.5 +- 2)
      data("quantile 0.25") should equal (-10.0 +- 20)
      data("quantile 0.50") should equal (17.5 +- 20)
      data("quantile 0.75") should equal (15.0 +- 1)
      data("quantile 0.90") should equal (22.5 +- 1)
      data("quantile 0.99") should equal (27.5 +- 1)
    }
  }

}
