package pythia.config

import pythia.dao._
import pythia.web.model.ModelMapper
import pythia.service.LocalClusterService

trait Bindings {

  implicit val componentBasePackage = "pythia"

  implicit val pipelineRepository = new PipelineRepository()
  implicit val componentRepository = new ComponentRepository()

  implicit val localClusterService = new LocalClusterService()

  implicit val modelMapper = new ModelMapper()
}
