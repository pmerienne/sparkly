package pythia.web.model

import pythia.core._
import pythia.dao.{VisualizationRepository, ComponentRepository}
import pythia.service.{ValidationMessage, ValidationReport, ClusterStatus}
import scala.util.Random

class ModelMapper(implicit val componentRepository: ComponentRepository, implicit val visualizationRepository: VisualizationRepository) {

  def convert(pipeline: PipelineConfiguration): PipelineConfigurationModel =  PipelineConfigurationModel (
    id = pipeline.id,
    name = pipeline.name,
    description = pipeline.description,
    components = pipeline.components.map(convert),
    connections = pipeline.connections.map(convert),
    visualizations = pipeline.visualizations.map(convert)
  )

  def convert(component: ComponentConfiguration): ComponentConfigurationModel = ComponentConfigurationModel (
    id = component.id,
    name = component.name,
    x = component.x,
    y = component.y,
    metadata = convert(component.clazz, componentRepository.findByClassName(component.clazz)),
    properties = component.properties.map(d => convert(d._1, d._2)).toList,
    inputs = component.inputs.map(d => convert(d._1, d._2)).toList,
    outputs =  component.outputs.map(d => convert(d._1, d._2)).toList
  )

  def convert(visualization: VisualizationConfiguration): VisualizationConfigurationModel = VisualizationConfigurationModel (
    id = visualization.id,  name = visualization.name,
    metadata = convert(visualization.clazz, visualizationRepository.findByClassName(visualization.clazz)),
    properties = visualization.properties.map(p => convert(p._1, p._2)).toList,
    streams = visualization.streams.map(p => convert(p._1, p._2)).toList,
    features = visualization.features.map(p => convert(p._1, p._2)).toList
  )

  def convert(id: String, metadata: ComponentMetadata): ComponentMetadataModel = ComponentMetadataModel (
    id = id,
    name = metadata.name,
    description = metadata.description,
    category = metadata.category,
    properties = metadata.properties.map(d => convert(d._1, d._2)).toList,
    inputs = metadata.inputs.map(d => convert(d._1, d._2)).toList,
    outputs = metadata.outputs.map(d => convert(d._1, d._2)).toList
  )

  def convert(id: String, metadata: VisualizationMetadata): VisualizationMetadataModel = VisualizationMetadataModel (
    id = id,
    name = metadata.name,
    properties = metadata.properties.map(d => convert(d._1, d._2)).toList,
    streams = metadata.streams,
    features = metadata.features
  )

  def convert(name: String, metadata: PropertyMetadata): PropertyMetadataModel = PropertyMetadataModel (
    name = name,
    propertyType = metadata.propertyType.toString,
    defaultValue = metadata.defaultValue,
    acceptedValues = metadata.acceptedValues,
    mandatory= metadata.mandatory,
    description = metadata.description
  )

  def convert(name: String, metadata: InputStreamMetadata): InputStreamMetadataModel = InputStreamMetadataModel (
    name = name,
    namedFeatures = metadata.namedFeatures.mapValues(_.toString()),
    listedFeatures = metadata.listedFeatures.mapValues(_.toString())
  )

  def convert(name: String, metadata: OutputStreamMetadata): OutputStreamMetadataModel = OutputStreamMetadataModel (
    name = name,
    from = metadata.from,
    namedFeatures = metadata.namedFeatures.mapValues(_.toString()),
    listedFeatures = metadata.listedFeatures.mapValues(_.toString())
  )

  def convert(name: String, value: String): PropertyConfigurationModel = PropertyConfigurationModel (
    name = name,
    value = value
  )

  def convert(name: String, configuration: StreamConfiguration): StreamConfigurationModel = StreamConfigurationModel (
    name = name,
    mappedFeatures =  configuration.mappedFeatures,
    selectedFeatures = configuration.selectedFeatures
  )

  def convert(name: String, identifier: StreamIdentifier): StreamIdentifierModel = StreamIdentifierModel (
    name = name,
    component = identifier.component,
    stream = identifier.stream
  )

  def convert(name: String, identifier: FeatureIdentifier): FeatureIdentifierModel = FeatureIdentifierModel (
    name = name,
    component = identifier.component,
    stream = identifier.stream,
    feature = identifier.feature
  )

  def convert(connection: ConnectionConfiguration): ConnectionModel = ConnectionModel (
    from = ConnectionPointModel(connection.from.component, connection.from.stream),
    to = ConnectionPointModel(connection.to.component, connection.to.stream)
  )

  def convert(pipeline: PipelineConfigurationModel): PipelineConfiguration =  PipelineConfiguration (
    id = pipeline.id,
    name = pipeline.name,
    description = pipeline.description,
    components = pipeline.components.map(convert),
    connections = pipeline.connections.map(convert),
    visualizations = pipeline.visualizations.map(convert)
  )

  def convert(component: ComponentConfigurationModel): ComponentConfiguration = ComponentConfiguration (
    id = component.id,
    name = component.name,
    x = component.x,
    y = component.y,
    clazz = component.metadata.id,
    properties = component.properties.map(convert).toMap,
    inputs = component.inputs.map(convert).toMap,
    outputs = component.outputs.map(convert).toMap
  )

  def convert(visualization: VisualizationConfigurationModel): VisualizationConfiguration = VisualizationConfiguration (
    id = visualization.id,
    name = visualization.name,
    clazz = visualization.metadata.id,
    properties = visualization.properties.map(convert).toMap,
    streams = visualization.streams.map(convert).toMap,
    features = visualization.features.map(convert).toMap
  )

  def convert(property: PropertyConfigurationModel) = (property.name, property.value)

  def convert(input: StreamConfigurationModel) = (input.name, StreamConfiguration (
    mappedFeatures = input.mappedFeatures,
    selectedFeatures = input.selectedFeatures
  ))

  def convert(identifier: StreamIdentifierModel) = (identifier.name, StreamIdentifier (
    component = identifier.component,
    stream = identifier.stream
  ))

  def convert(identifier: FeatureIdentifierModel) = (identifier.name, FeatureIdentifier (
    component = identifier.component,
    stream = identifier.stream,
    feature = identifier.feature
  ))

  def convert(connection: ConnectionModel): ConnectionConfiguration = ConnectionConfiguration (
    from = ConnectionPoint(connection.from.component, connection.from.stream),
    to = ConnectionPoint(connection.to.component, connection.to.stream)
  )
  
  def convert(status: ClusterStatus): ClusterStatusModel = ClusterStatusModel (
    state = status.action.toString,
    time = status.time,
    pipeline = status.pipeline.map(pipeline => convert(pipeline))
  )

  def convert(report: ValidationReport): ValidationReportModel = ValidationReportModel (
    messages = report.messages.map(message => convert(message))
  )

  def convert(message: ValidationMessage): ValidationMessageModel = ValidationMessageModel (
    text = message.text,
    level = message.level.toString
  )
}
