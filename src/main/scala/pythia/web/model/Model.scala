package pythia.web.model

import java.util.Date

case class PipelineConfigurationModel (
  id: String,
  name: String,
  components: List[ComponentConfigurationModel] = List(),
  connections: List[ConnectionModel] = List()
)

case class ConnectionModel(from: ConnectionPointModel, to: ConnectionPointModel) 
case class ConnectionPointModel(component: String, stream: String)

case class ComponentConfigurationModel (
  id: String,
  name: String, x: Int = 0, y: Int = 0,
  metadata: ComponentMetadataModel,
  properties: List[PropertyConfigurationModel] = List(),
  inputs: List[StreamConfigurationModel] = List(),
  outputs: List[StreamConfigurationModel] = List()
)

case class StreamConfigurationModel (
  name: String,
  mappedFeatures: Map[String, String] = Map(),
  selectedFeatures: Map[String, List[String]] = Map()
)


case class ComponentMetadataModel (
  id: String,
  name: String,
  description: String = "", category: String = "Others",
  properties: List[PropertyMetadataModel] = List(),
  inputs: List[InputStreamMetadataModel] = List(),
  outputs: List[OutputStreamMetadataModel] = List()
)

case class PropertyMetadataModel (
  name: String,
  propertyType: String,
  defaultValue: Option[_] = None,
  acceptedValues: List[String] = List(),
  mandatory: Boolean = true
)

case class InputStreamMetadataModel(name: String, namedFeatures: List[String] = List(),listedFeatures: List[String] = List())

case class OutputStreamMetadataModel(name: String, from: Option[String] = None, namedFeatures: List[String] = List(), listedFeatures: List[String] = List())

case class PropertyConfigurationModel(name: String, value: String)

case class ClusterModel(id: String, name: String, status: ClusterStatusModel)
case class ClusterStatusModel(state: String, time: Option[Date], pipeline: Option[PipelineConfigurationModel])