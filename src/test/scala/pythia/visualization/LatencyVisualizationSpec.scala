package pythia.visualization

import pythia.core._

class LatencyVisualizationSpec extends VisualizationSpec {

  "LatencyVisualization" should "send pipeline latency" in {
    // Given
    val datas = (1 to 100).map(i => Instance("index" -> i)).toList

    // When
    launchVisualization(classOf[LatencyVisualization])
    stream.push(datas)

    // Then
    eventually {
      val data = latestSentOutData()
      println(data)
      data("processingDelay") should be < 500.0
      data("schedulingDelay") should be < 500.0
      data("totalDelay") should be equals data("processingDelay") + data("schedulingDelay")
    }
  }


}
