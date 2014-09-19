package pythia.core

import scala.util.Random

case class PipelineConfiguration (
  id: String = Random.nextString(10),
  name: String,
  components: List[ComponentConfiguration] = List(),
  connections: List[ConnectionConfiguration] = List()
)

case class ConnectionConfiguration(from: ConnectionPoint, to: ConnectionPoint) {
  def isTo(component: String, stream: String) = to == ConnectionPoint(component, stream)
  def isFrom(component: String, stream: String) = from == ConnectionPoint(component, stream)
}
case class ConnectionPoint(component: String, stream: String)

case class ComponentConfiguration (
  id: String = Random.nextString(10),
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