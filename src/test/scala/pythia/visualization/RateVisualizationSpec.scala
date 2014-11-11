package pythia.visualization

import pythia.core._
import pythia.testing.MockStream

class RateVisualizationSpec extends VisualizationSpec {

  "RateVisualization" should "send stream's rate" in {
    // Given
    val data = (1 to 100).map(i => Instance("index" -> i)).toList
    val stream = MockStream(ssc)
    outputStreams += ("component", "stream") -> stream.dstream

    val configuration = VisualizationConfiguration (
      name = "Instance rate", clazz = classOf[RateVisualization].getName,
      properties = Map("Window length (in ms)" -> "1000"),
      streams = Map("Stream" -> StreamIdentifier("component", "stream"))
    )

    // When
    launchVisualization(configuration)
    stream.push(data)

    // Then
    eventually {
      val data = latestSentOutData()
      data("rate") should be equals(100.0)
    }
  }

}
