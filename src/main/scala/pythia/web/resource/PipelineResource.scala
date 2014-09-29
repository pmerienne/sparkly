package pythia.web.resource

import pythia.dao.PipelineRepository
import pythia.service.PipelineValidationService
import pythia.web.model.{ModelMapper, PipelineConfigurationModel}

class PipelineResource(
  implicit val pipelineRepository: PipelineRepository,
  implicit val pipelineValidationService: PipelineValidationService,
  implicit val modelMapper: ModelMapper) extends BaseResource {

  get("/") {
    pipelineRepository
      .all()
      .map(modelMapper.convert(_))
  }

  get("/:id") {
    val id = params("id")
    pipelineRepository.get(id) match {
      case Some(pipeline) => modelMapper.convert(pipeline)
      case None => halt(404)
    }
  }

  get("/:id/validation-report") {
    val id = params("id")
    pipelineRepository.get(id) match {
      case None => halt(404)
      case Some(pipeline) => {
        val report = pipelineValidationService.validate(pipeline)
        modelMapper.convert(report)
      }
    }
  }

  delete("/:id") {
    val id = params("id")
    pipelineRepository.delete(id)
  }

  put("/") {
    val pipeline = parsedBody.extract[PipelineConfigurationModel]
    pipelineRepository.store(pipeline.id, modelMapper.convert(pipeline))
  }

}