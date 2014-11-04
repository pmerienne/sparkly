package pythia.core

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.StreamingContext
import pythia.visualization.LatencyVisualization
import java.net.InetAddress
import pythia.config.PythiaConfig

class VisualizationBuilder {

  val visualizationHost = InetAddress.getLocalHost.getHostName
  val visualizationPort = PythiaConfig.WEB_PORT

  def buildVisualizations(context: StreamingContext, configuration: PipelineConfiguration, outputStreams: Map[(String, String), DStream[Instance]]): Unit = {
    // Global viz
    val visualizationClient = new VisualizationClient(visualizationHost, visualizationPort, context.sparkContext.master, "pipeline-latency")
    val baseVisualization = new LatencyVisualization()
    baseVisualization.init(null, context, visualizationClient)


  }
}
