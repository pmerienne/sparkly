package pythia.web.resource

import pythia.service.LocalClusterService
import pythia.web.model.ModelMapper

class LocalClusterResource(
  implicit val localClusterService: LocalClusterService,
  implicit val modelMapper: ModelMapper) extends BaseResource {

  get("/status") {
    val status = localClusterService.status
    modelMapper.convert(status)
  }

  put("/") {
    val pipelineId = params("id")
    localClusterService.deploy(pipelineId)
  }
}
