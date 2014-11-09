package pythia.core

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.StreamingContext
import pythia.visualization.LatencyVisualization
import java.net.InetAddress
import pythia.config.PythiaConfig

class VisualizationBuilder {

  val visualizationHost = PythiaConfig.HOSTNAME
  val visualizationPort = PythiaConfig.WEB_PORT

  def buildVisualizations(clusterId: String, context: StreamingContext, configuration: PipelineConfiguration, outputStreams: Map[(String, String), DStream[Instance]]): Unit = {
    // Memory viz is created by spark using spark metrics!
    // Global viz
    val visualizationClient = new VisualizationClient(visualizationHost, visualizationPort, clusterId, "pipeline-latency")
    val baseVisualization = new LatencyVisualization()
    baseVisualization.init(null, context, visualizationClient)


  }
}
