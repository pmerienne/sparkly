package sparkly.config

import sparkly.dao._
import sparkly.service._
import sparkly.web.model.ModelMapper

trait Bindings {

  val componentBasePackage = "sparkly"
  val visualizationBasePackage = "sparkly.visualization"

  implicit val pipelineRepository = new PipelineRepository()
  implicit val componentRepository = new ComponentRepository(componentBasePackage)
  implicit val visualizationRepository = new VisualizationRepository(visualizationBasePackage)

  implicit val pipelineValidationService = new PipelineValidationService()
  implicit val localClusterService = new LocalClusterService()

  implicit val modelMapper = new ModelMapper()
}
