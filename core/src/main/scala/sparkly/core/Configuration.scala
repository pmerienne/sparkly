package sparkly.core

import sparkly.core.SparkDefaultConfiguration.defaultSettings

import scala.util.Random

case class PipelineConfiguration (
  id: String = Random.alphanumeric.take(10).mkString,
  name: String, description: String = "",
  components: List[ComponentConfiguration] = List(),
  connections: List[ConnectionConfiguration] = List(),
  visualizations: List[VisualizationConfiguration] = List(),
  settings: Map[String, Map[String, String]] = defaultSettings
)

case class ConnectionConfiguration(from: ConnectionPoint, to: ConnectionPoint) {
  def isTo(component: String, stream: String) = to == ConnectionPoint(component, stream)
  def isFrom(component: String, stream: String) = from == ConnectionPoint(component, stream)
}

case class ConnectionPoint(component: String, stream: String)

case class ComponentConfiguration (
  id: String = Random.alphanumeric.take(10).mkString,
  name: String, x: Int = 0, y: Int = 0,
  clazz: String,
  properties: Map[String, String] = Map(),
  inputs: Map[String, StreamConfiguration] = Map(),
  outputs: Map[String, StreamConfiguration] = Map()
)

case class StreamConfiguration(mappedFeatures: Map[String, String] = Map(), selectedFeatures: Map[String, List[String]] = Map())

object ConnectionConfiguration {
  def apply(fromComponent: String, fromStream: String, toComponent: String, toStream: String) = {
    new ConnectionConfiguration(ConnectionPoint(fromComponent, fromStream), ConnectionPoint(toComponent, toStream))
  }
}

case class VisualizationConfiguration (
  id: String = Random.alphanumeric.take(10).mkString,  name: String,
  clazz: String,
  properties: Map[String, String] = Map(),
  streams: Map[String, StreamIdentifier] = Map(),
  features: Map[String, FeatureIdentifier] = Map()
)

case class StreamIdentifier(component: String, stream: String) {
  def hasMissingFields(): Boolean = Option(component).getOrElse("").isEmpty || Option(stream).getOrElse("").isEmpty
}
case class FeatureIdentifier(component: String, stream: String, feature: String) {
  def hasMissingFields(): Boolean = Option(component).getOrElse("").isEmpty || Option(stream).getOrElse("").isEmpty || Option(feature).getOrElse("").isEmpty
}
