package pythia.visualization

import pythia.core._
import pythia.testing.MockStream

class ThroughputVisualizationSpec extends VisualizationSpec {

  "ThroughputVisualization" should "send stream's throughput" in {
    // Given
    val data = (1 to 100).map(i => Instance("index" -> i)).toList
    val stream = MockStream(ssc)
    outputStreams += ("component", "stream") -> stream.dstream

    val configuration = VisualizationConfiguration (
      name = "Instance throughput", clazz = classOf[ThroughputVisualization].getName,
      properties = Map("Window length (in ms)" -> "1000"),
      streams = Map("Stream" -> StreamIdentifier("component", "stream"))
    )

    // When
    launchVisualization(configuration)
    stream.push(data)

    // Then
    eventually {
      val data = latestSentOutData()
      data("throughput") should be equals(100.0)
    }
  }

}
