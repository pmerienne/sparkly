package pythia.core

import pythia.core.PropertyType.PropertyType

case class ComponentMetadata (
  name: String,
  description: String = "", category: String = "Others",
  properties: Map[String, PropertyMetadata] = Map(),
  inputs: Map[String, InputStreamMetadata] = Map(),
  outputs: Map[String, OutputStreamMetadata] = Map()
)

case class PropertyMetadata (
  propertyType: PropertyType,
  defaultValue: Option[_] = None,
  acceptedValues: List[String] = List(),
  mandatory: Boolean = true) {
}

object PropertyType extends Enumeration {
  type PropertyType = Value
  val DECIMAL, STRING, INTEGER, LONG, BOOLEAN  = Value
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