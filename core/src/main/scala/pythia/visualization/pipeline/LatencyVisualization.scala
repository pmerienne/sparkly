package pythia.visualization.pipeline

import pythia.core.VisualizationDataCollector
import org.apache.spark.streaming.scheduler.{StreamingListenerBatchCompleted, StreamingListener}
import org.apache.spark.streaming.StreamingContext

object LatencyVisualization {

  def build(ssc: StreamingContext, visualizationHost: String, visualizationPort: Int, clusterId: String) = {
    val dataCollector = new VisualizationDataCollector(visualizationHost, visualizationPort, clusterId, "pipeline-latency")

    ssc.addStreamingListener(new StreamingListener {
      override def onBatchCompleted(batchComplete: StreamingListenerBatchCompleted) = {
        dataCollector.push(batchComplete.batchInfo.batchTime.milliseconds, Map(
          "processingDelay" -> batchComplete.batchInfo.processingDelay.getOrElse(0L).toDouble,
          "schedulingDelay" -> batchComplete.batchInfo.schedulingDelay.getOrElse(0L).toDouble,
          "totalDelay" -> batchComplete.batchInfo.totalDelay.getOrElse(0L).toDouble
        ))
      }
    })
  }
}
