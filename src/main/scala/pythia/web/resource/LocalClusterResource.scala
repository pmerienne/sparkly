package pythia.web.resource

import pythia.service.LocalClusterService
import pythia.web.model.{ClusterModel, ModelMapper}

class LocalClusterResource(
  implicit val localClusterService: LocalClusterService,
  implicit val modelMapper: ModelMapper) extends BaseResource {

  get("/") {
    val status = modelMapper.convert(localClusterService.status)
    ClusterModel("local", "Local", status)
  }

  get("/status") {
    val status = localClusterService.status
    modelMapper.convert(status)
  }


  post("/:action") {
    params("action") match {
      case "deploy" => localClusterService.deploy(params("pipelineId"))
      case "stop" => localClusterService.stop(params.getOrElse("stopGracefully", "false").toBoolean)
      case _ => throw new IllegalArgumentException("Unknonw action " + params("action"))
    }

  }
}
