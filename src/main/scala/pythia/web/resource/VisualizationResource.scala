package pythia.web.resource

import org.scalatra.atmosphere._
import scala.concurrent.ExecutionContext.Implicits.global

class VisualizationResource extends BaseResource with AtmosphereSupport {

  atmosphere("/:master/:id") {
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