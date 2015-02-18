package sparkly.visualization

import sparkly.core._

class ThroughputVisualizationSpec extends VisualizationSpec {

  "ThroughputVisualization" should "send stream's throughput" in {
    // Given
    val configuration = VisualizationConfiguration (
      name = "Instance throughput", clazz = classOf[ThroughputVisualization].getName,
      properties = Map("Window length (in ms)" -> "1000"),
      streams = Map("Stream" -> StreamIdentifier("component", "stream"))
    )

    // When
    val build = deployVisualization(configuration)

    val data = (1 to 100).map(i => Instance("index" -> i)).toList
    build.mockStream("component", "stream").push(data)

    // Then
    eventually {
      val data = latestSentOutData()
      data("throughput") should be equals(100.0)
    }
  }

}
