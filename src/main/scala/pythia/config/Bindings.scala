package pythia.config

import pythia.dao.{ComponentRepository, PipelineRepository}
import pythia.web.model.ModelMapper

trait Bindings {

  implicit val componentBasePackage = "pythia"

  implicit val pipelineRepository = new PipelineRepository()
  implicit val componentRepository = new ComponentRepository()

  implicit val modelMapper = new ModelMapper()
}
