package sparkly.core

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

abstract class Visualization extends Serializable {
  def metadata: VisualizationMetadata
  def init(context: VisualizationContext): Unit
}

case class VisualizationContext (
  ssc: StreamingContext, dataCollector: VisualizationDataCollector,
  streams: Map[String, DStream[Instance]],
  features: Map[String, DStream[Feature[Any]]],
  properties: Map[String, Property]
) {
  val sc = ssc.sparkContext
  def property(name: String) = properties(name)
}