package pythia.service

import pythia.core._
import pythia.dao._
import pythia.service.MessageLevel.MessageLevel

import scala.util.Try
import pythia.core.StreamConfiguration
import pythia.core.ComponentConfiguration
import pythia.core.PipelineConfiguration
import scala.Some

class PipelineValidationService(
  implicit val visualizationRepository: VisualizationRepository,
  implicit val componentRepository: ComponentRepository) {

  val componentValidator = new ComponentValidator()
  val visualizationValidator = new VisualizationValidator()

  def validate(pipeline: PipelineConfiguration): ValidationReport = {
    val componentsMessages = pipeline.components.flatMap(componentValidator.validate)
    val visualizationsMessages = pipeline.visualizations.flatMap(visualizationValidator.validate)

    ValidationReport(componentsMessages ++ visualizationsMessages)
  }
}

class ComponentValidator(implicit val componentRepository: ComponentRepository) {
  def validate(componentConfiguration: ComponentConfiguration): List[ValidationMessage] = {
    val componentName = componentConfiguration.name

    val componentMetadata = componentRepository.component(componentConfiguration.clazz) match {
      case Some(metadata) => metadata
      case None => {
        return List(ValidationMessage(s"'${componentName}' component implementation '${componentConfiguration.clazz}' is unknown.", MessageLevel.Error))
      }
    }

    val missingPropertiesReport = componentMetadata.properties
      .filter{case (name, metadata) => metadata.mandatory && metadata.defaultValue.isEmpty && !componentConfiguration.properties.contains(name)}
      .map{case (name, metadata) => ValidationMessage(s"'${name}' property of '${componentName}' component is mandatory.", MessageLevel.Error)}

    val badPropertyValuesReport = componentMetadata.properties
      .map{case (name, metadata) => (name, metadata, componentConfiguration.properties.get(name))}
      .filter{case (name, metadata, value) =>
        val property = Property(metadata, value)
        Try(property.get).isFailure
      }
      .map{case (name, metadata, value) => ValidationMessage(s"'${name}' property of '${componentName}' component is a ${metadata.propertyType} property. Value '${value.getOrElse(null)}' is not supported.", MessageLevel.Error)}

    val missingInputMappings = componentMetadata.inputs
      .flatMap{case (name, metadata) =>
        val mappedFeatures = componentConfiguration.inputs.getOrElse(name, StreamConfiguration()).mappedFeatures.keys.toList
        metadata.namedFeatures.keys.filter(!mappedFeatures.contains(_)).map(missingMapping => (name, metadata, missingMapping))
      }.map{case (name, metadata, missingMapping) =>
        ValidationMessage(s"Missing mapping '${missingMapping}' of input '${name}' in '${componentName}' component.", MessageLevel.Error)
      }

    val missingOutputMappings = componentMetadata.outputs
      .flatMap{case (name, metadata) =>
        val mappedFeatures = componentConfiguration.outputs.getOrElse(name, StreamConfiguration()).mappedFeatures.keys.toList
        metadata.namedFeatures.keys.filter(!mappedFeatures.contains(_)).map(missingMapping => (name, metadata, missingMapping))
      }.map{case (name, metadata, missingMapping) =>
        ValidationMessage(s"Missing mapping '${missingMapping}' of output '${name}' in '${componentName}' component.", MessageLevel.Error)
      }

    missingPropertiesReport.toList ++ badPropertyValuesReport ++ missingInputMappings ++ missingOutputMappings
  }
}


class VisualizationValidator(implicit val visualizationRepository: VisualizationRepository) {
  def validate(visualizationConfiguration: VisualizationConfiguration): List[ValidationMessage] = {
    val visualizationName = visualizationConfiguration.name

    val visualizationMetadata = visualizationRepository.visualization(visualizationConfiguration.clazz) match {
      case Some(metadata) => metadata
      case None => {
        return List(ValidationMessage(s"'${visualizationName}' visualization implementation '${visualizationConfiguration.clazz}' is unknown.", MessageLevel.Error))
      }
    }

    val missingPropertiesReport = visualizationMetadata.properties
      .filter{case (name, metadata) => metadata.mandatory && metadata.defaultValue.isEmpty && !visualizationConfiguration.properties.contains(name)}
      .map{case (name, metadata) => ValidationMessage(s"'${name}' property of '${visualizationName}' visualization is mandatory.", MessageLevel.Error)}
      .toList

    val badPropertyValuesReport = visualizationMetadata.properties
      .map{case (name, metadata) => (name, metadata, visualizationConfiguration.properties.get(name))}
      .filter{case (name, metadata, value) =>
        val property = Property(metadata, value)
        Try(property.get).isFailure
      }
      .map{case (name, metadata, value) => ValidationMessage(s"'${name}' property of '${visualizationName}' visualization is a ${metadata.propertyType} property. Value '${value.getOrElse(null)}' is not supported.", MessageLevel.Error)}

    val missingStreamReport = visualizationMetadata.streams
      .filter(stream => !visualizationConfiguration.streams.contains(stream))
      .map(stream => ValidationMessage(s"'${stream}' stream of '${visualizationName}' visualization is missing.", MessageLevel.Error))

    val badStreamReport = visualizationMetadata.streams
      .filter(stream => visualizationConfiguration.streams.contains(stream))
      .map{stream => (stream, visualizationConfiguration.streams(stream))}
      .filter{case (stream, identifier) => identifier.hasMissingFields()}
      .map{case (stream, identifier) => ValidationMessage(s"'${stream}' stream of '${visualizationName}' visualization is missing.", MessageLevel.Error)}

    val missingFeatureReport = visualizationMetadata.features
      .filter(feature => !visualizationConfiguration.features.contains(feature))
      .map(feature => ValidationMessage(s"'${feature}' feature of '${visualizationName}' visualization is missing.", MessageLevel.Error))

    val badFeatureReport = visualizationMetadata.features
      .filter(feature => visualizationConfiguration.features.contains(feature))
      .map{feature => (feature, visualizationConfiguration.features(feature))}
      .filter{case (feature, identifier) => identifier.hasMissingFields()}
      .map{case (feature, identifier) => ValidationMessage(s"'${feature}' feature of '${visualizationName}' visualization is missing.", MessageLevel.Error)}

    missingPropertiesReport ++ badPropertyValuesReport ++ missingStreamReport ++ badStreamReport ++ missingFeatureReport ++ badFeatureReport
  }
}

case class ValidationReport(messages: List[ValidationMessage] = List()) {
  def add(message: ValidationMessage): ValidationReport = this.copy(messages = this.messages :+ message)
  def add(text: String, level: MessageLevel): ValidationReport = add(ValidationMessage(text, level))

  def containsErrors() = messages.find(_.level == MessageLevel.Error).isDefined
}

case class ValidationMessage(text: String, level: MessageLevel)

object MessageLevel extends Enumeration {
  type MessageLevel = Value
  val Warning, Error = Value
}