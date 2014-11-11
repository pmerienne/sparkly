package pythia.web.resource

import org.scalatra.atmosphere._

import scala.concurrent.ExecutionContext.Implicits.global
import pythia.dao.{VisualizationRepository, ComponentRepository}
import pythia.web.model.ModelMapper

class VisualizationResource(
  implicit val visualizationRepository: VisualizationRepository,
  implicit val modelMapper: ModelMapper) extends BaseResource with AtmosphereSupport {

  get("/") {
    visualizationRepository
      .visualizations()
      .map{case (id, metadata) => (id, modelMapper.convert(id, metadata))}
  }

  get("/:id") {
    val id = params("id")
    visualizationRepository.visualization(id) match {
      case Some(metadata) => modelMapper.convert(id, metadata)
      case None => halt(404)
    }
  }

  atmosphere("/data/:master/:id") {
    val master = params("master")
    val id = params("id")
    new VisualizationAtmosphereClient(master + ":" + id)
  }

  class VisualizationAtmosphereClient(val id: String) extends AtmosphereClient {

    def receive: AtmoReceive = {
      case JsonMessage(json) => broadcastEvent(json)
      case _ =>
    }

    def broadcastEvent(msg: OutboundMessage) {
      try {
        broadcast(msg, CustomClientFilter)
      } catch {
        case e: NoSuchMethodError => // java.lang.NoSuchMethodError: akka.actor.ActorSystem.dispatcher()Lscala/concurrent/ExecutionContextExecutor;
        // But it should work!
      }
    }

    val CustomClientFilter = new ClientFilter(null) {
      def apply(client: AtmosphereClient) = client.asInstanceOf[VisualizationAtmosphereClient].id == id
      override def toString(): String = "CustomClientFilter"
    }
  }
}