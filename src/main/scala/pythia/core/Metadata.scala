package pythia.core

import pythia.core.FeatureType.FeatureType
import pythia.core.PropertyType.PropertyType

case class ComponentMetadata (
  name: String,
  description: String = "", category: String = "Others",
  properties: Map[String, PropertyMetadata] = Map(),
  inputs: Map[String, InputStreamMetadata] = Map(),
  outputs: Map[String, OutputStreamMetadata] = Map()
)

case class VisualizationMetadata (
  name: String,
  properties: Map[String, PropertyMetadata] = Map(),
  streams: List[String] = List(),
  features: List[String] = List()
)

case class PropertyMetadata (
  propertyType: PropertyType,
  defaultValue: Option[_] = None,
  acceptedValues: List[String] = List(),
  mandatory: Boolean = true,
  description: String = ""
)

object PropertyType extends Enumeration {
  type PropertyType = Value
  val DECIMAL, STRING, INTEGER, LONG, BOOLEAN  = Value
}

case class InputStreamMetadata (
  namedFeatures: Map[String, FeatureType] = Map(),
  listedFeatures: Map[String, FeatureType] = Map()
)

case class OutputStreamMetadata (
  from: Option[String] = None,
  namedFeatures: Map[String, FeatureType] = Map(),
  listedFeatures: Map[String, FeatureType] = Map()
)

object FeatureType extends Enumeration {
  type FeatureType = Value
  val STRING, DOUBLE, INTEGER, LONG, NUMBER, BOOLEAN , DATE, ANY = Value
}
