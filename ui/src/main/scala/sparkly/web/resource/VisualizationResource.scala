package sparkly.web.resource

import com.google.common.collect.EvictingQueue
import org.json4s._
import org.scalatra.atmosphere.{JsonMessage, _}
import sparkly.dao.VisualizationRepository
import sparkly.web.model.ModelMapper

import scala.collection.mutable.{Map => MutableMap}
import scala.concurrent.ExecutionContext.Implicits.global

class VisualizationResource(
  implicit val visualizationRepository: VisualizationRepository,
  implicit val modelMapper: ModelMapper) extends BaseResource with AtmosphereSupport {

  val visualizationData: MutableMap[String, VisualizationData] = MutableMap()

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

  get("/past_data/:master/:id") {
    val master = params("master")
    val id = params("id")
    val visualizationId = master + ":" + id
    getVisualizationData(visualizationId).all()
  }

  atmosphere("/data/:master/:id") {
    val master = params("master")
    val id = params("id")
    val visualizationId = master + ":" + id
    val data = getVisualizationData(visualizationId)
    new VisualizationAtmosphereClient(visualizationId, data)
  }

  private def getVisualizationData(id: String) = visualizationData.get(id) match {
    case Some(data) => data
    case None => {
      val data = new VisualizationData()
      visualizationData.put(id, data)
      data
    }
  }

  class VisualizationAtmosphereClient(val id: String, val data: VisualizationData) extends AtmosphereClient {

    def receive: AtmoReceive = {
      case JsonMessage(json) => {
        data.add(json)
        broadcastEvent(json)
      }
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

class VisualizationData {
  import scala.collection.JavaConversions._

  private val MAX_DATA = 100
  private val queue = EvictingQueue.create[JValue](MAX_DATA)

  def add(json: JValue) = queue.offer(json)
  def all() = queue.toList
}