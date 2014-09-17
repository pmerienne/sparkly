package pythia.model

import pythia.core.Metadata

case class ComponentConfig(
  id: String, name: String,
  x: Int, y: Int,
  metadata: Metadata,
  properties: List[PropertyConfig] = List(),
  inputStreams: List[StreamConfig] = List(),
  outputStreams: List[StreamConfig] = List()
)

case class PropertyConfig(name: String, value: String)
case class StreamConfig(name: String, mappedFeatures: Map[String, String] = Map(), selectedFeatures: Map[String, List[String]] = Map())