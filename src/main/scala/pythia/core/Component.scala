package pythia.core

abstract class Component {
  def metadata: Metadata
}

case class Metadata(
  name: String, description: String = "",
  properties: List[PropertyMetadata] = List(),
  inputStreams: List[StreamMetadata] = List(),
  outputStreams: List[StreamMetadata] = List()
)

case class PropertyMetadata(name: String, propertyType: String, defaultValue: String = "", mandatory: Boolean = false, acceptedValues: List[String] = List())
case class StreamMetadata(name: String, from: String = null, listedFeatures: List[String] = List(), namedFeatures: List[String] = List())