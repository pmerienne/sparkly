package pythia.core

import org.atmosphere.wasync.impl.{DefaultOptions, DefaultOptionsBuilder, DefaultRequestBuilder}
import org.atmosphere.wasync.{Socket, Client, ClientFactory, Request}
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.concurrent.Future

class VisualizationClient(val hostname: String, val port: Int, val clusterId:String , val id: String) {

  import scala.concurrent.ExecutionContext.Implicits.global
  implicit val formats = DefaultFormats

  val url = s"ws://${hostname}:${port}/api/visualization/${clusterId}/${id}"

  def send(timestamp: Long, data: Map[String, Double]): Unit = send(VisualizationEvent(timestamp, data), true)
  def send(timestamp: Long, data: Map[String, Double], async: Boolean): Unit = send(VisualizationEvent(timestamp, data), async)

  private def send(event: VisualizationEvent, async: Boolean): Unit = {
    val json = compact(Extraction.decompose(event))

    async match {
      case true =>  Future { sendJson(json) }
      case false => sendJson(json)
    }
  }


  private def sendJson(json: String): Unit = {
    val socket = VisualizationClient.getOrCreateSocket(url)
    socket.fire(json)
  }

}

object VisualizationClient {
  import scala.collection.mutable.Map

  private val client: Client[DefaultOptions, DefaultOptionsBuilder, DefaultRequestBuilder] = ClientFactory.getDefault.newClient.asInstanceOf[Client[DefaultOptions, DefaultOptionsBuilder, DefaultRequestBuilder]]
  private val opts = client.newOptionsBuilder().reconnect(false).build()

  private val sockets: Map[String, Socket] = Map()
  sys.addShutdownHook{ sockets.map(_._2).foreach(_.close()) }

  protected def getOrCreateSocket(url: String): Socket = sockets.get(url) match {
    case Some(socket) => socket
    case None => {
      val req = client.newRequestBuilder.method(Request.METHOD.GET).uri(url).transport(Request.TRANSPORT.WEBSOCKET)
      val socket = client.create(opts).open(req.build())
      sockets += url -> socket

      socket
    }
  }
}