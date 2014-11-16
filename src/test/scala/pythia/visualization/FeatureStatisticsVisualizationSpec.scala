package pythia.visualization

import pythia.core._
import pythia.testing.MockStream

class FeatureStatisticsVisualizationSpec extends VisualizationSpec {

  "FeatureStatisticsVisualization" should "send distinct feature's statistics" in {
    // Given
    val data = List(-10, 15, null, -5, 20, 25, -10, 15, 10, null).map(value => Instance("value" -> value)).toList
    val stream = MockStream(ssc)
    outputStreams += ("component", "stream") -> stream.dstream

    val configuration = VisualizationConfiguration (
      name = "Feature statistics", clazz = classOf[FeatureStatisticsVisualization].getName,
      properties = Map("Window length (in ms)" -> "1000"),
      features = Map("Number feature" -> FeatureIdentifier("component", "stream", "value"))
    )

    // When
    launchVisualization(configuration)
    stream.push(data)

    // Then
    eventually {
      val data = latestSentOutData()
      data should contain only (
        "count" -> 10.0, "missing" -> 2.0,
        "mean" -> 7.5, "std" -> 12.99038105676658,
        "min" -> -10.0, "max" -> 27.5,
        "quantile 0.25" -> -10.0, "quantile 0.50" -> 17.5, "quantile 0.75" -> 15.0, "quantile 0.90" -> 27.5, "quantile 0.99" -> 27.5
      )
    }
  }

}
