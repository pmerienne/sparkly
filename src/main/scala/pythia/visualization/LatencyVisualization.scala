package pythia.visualization

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}
import pythia.core.{VisualizationClient, Instance}

class LatencyVisualization extends Visualization {

  override def init(stream: DStream[Instance], ssc: StreamingContext, client: VisualizationClient) {
    ssc.addStreamingListener(new StreamingListener {
      override def onBatchCompleted(batchComplete: StreamingListenerBatchCompleted) = {
        client.send(batchComplete.batchInfo.batchTime.milliseconds, Map (
          "processingDelay" ->  batchComplete.batchInfo.processingDelay.getOrElse(0L).toDouble,
          "schedulingDelay" ->  batchComplete.batchInfo.schedulingDelay.getOrElse(0L).toDouble,
          "totalDelay" ->  batchComplete.batchInfo.totalDelay.getOrElse(0L).toDouble
        ))
      }
    })
  }
}
