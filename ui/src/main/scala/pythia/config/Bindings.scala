package pythia.config

import pythia.dao._
import pythia.service._
import pythia.web.model.ModelMapper

trait Bindings {

  val componentBasePackage = "pythia"
  val visualizationBasePackage = "pythia.visualization"

  implicit val pipelineRepository = new PipelineRepository()
  implicit val componentRepository = new ComponentRepository(componentBasePackage)
  implicit val visualizationRepository = new VisualizationRepository(visualizationBasePackage)

  implicit val pipelineValidationService = new PipelineValidationService()
  implicit val localClusterService = new LocalClusterService()

  implicit val modelMapper = new ModelMapper()
}
