package sparkly.core

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.StreamingContext
import sparkly.visualization.pipeline.LatencyVisualization

class VisualizationBuilder(val visualizationHost: String, val visualizationPort: Int) {

  def buildVisualizations(clusterId: String, ssc: StreamingContext, configuration: PipelineConfiguration, outputStreams: Map[(String, String), DStream[Instance]]): Unit = {
    initBaseVisualizations(clusterId, ssc)

    configuration.visualizations.foreach{configuration =>
      val visualization = Class.forName(configuration.clazz).newInstance.asInstanceOf[Visualization]

      val dataCollector = new VisualizationDataCollector(visualizationHost, visualizationPort, clusterId, configuration.id)
      val context = buildContext(ssc, dataCollector, visualization.metadata, configuration, outputStreams)

      visualization.init(context)
    }

  }

  def buildContext(ssc: StreamingContext, dataCollector: VisualizationDataCollector, metadata: VisualizationMetadata, configuration: VisualizationConfiguration, outputStreams: Map[(String, String), DStream[Instance]]): VisualizationContext = {
      val streams = metadata.streams.map{ streamName =>
        val streamIdentifier = configuration.streams(streamName)
        val instanceStream = outputStreams((streamIdentifier.component, streamIdentifier.stream))
        (streamName, instanceStream)
      }.toMap

      val features = metadata.features.map { featureName =>
        val featureIdentifier = configuration.features(featureName)
        val featureStream = outputStreams((featureIdentifier.component, featureIdentifier.stream)).map(_.rawFeature(featureIdentifier.feature))
        (featureName, featureStream)
      }.toMap

      val properties = metadata.properties.map{prop =>
        val value = configuration.properties.get(prop._1)
        (prop._1, Property(prop._2, value))
      }.toMap

      VisualizationContext(ssc, dataCollector, streams, features, properties)
  }

  private def initBaseVisualizations(clusterId: String, ssc: StreamingContext): Unit = {
    // Memory viz is created by spark using spark metrics!
    // Latency Visualization
    LatencyVisualization.build(ssc, visualizationHost, visualizationPort, clusterId)
  }
}