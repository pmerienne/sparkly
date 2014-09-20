package pythia.core

case class ComponentMetadata (
  name: String,
  description: String = "", category: String = "Others",
  properties: Map[String, PropertyMetadata] = Map(),
  inputs: Map[String, InputStreamMetadata] = Map(),
  outputs: Map[String, OutputStreamMetadata] = Map()
)

case class PropertyMetadata (
  propertyType: String,
  defaultValue: Option[_] = None,
  acceptedValues: List[String] = List(),
  mandatory: Boolean = true) {
}

case class InputStreamMetadata (
  namedFeatures: List[String] = List(),
  listedFeatures: List[String] = List()
)

case class OutputStreamMetadata (
  from: Option[String] = None,
  namedFeatures: List[String] = List(),
  listedFeatures: List[String] = List()
)