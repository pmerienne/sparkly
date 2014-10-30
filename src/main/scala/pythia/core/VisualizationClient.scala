package pythia.core

import org.atmosphere.wasync.impl.{DefaultOptions, DefaultOptionsBuilder, DefaultRequestBuilder}
import org.atmosphere.wasync.{Client, ClientFactory, Request}
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.concurrent.Future

class VisualizationClient(val hostname: String, val port: Int, val master:String , val id: String) {

  import scala.concurrent.ExecutionContext.Implicits.global
  implicit val formats = DefaultFormats

  val url = s"ws://${hostname}:${port}/api/visualization/${clean(master)}/${clean(id)}"

  def send(timestamp: Long, data: Map[String, Double]): Unit = send(VisualizationEvent(timestamp, data))

  def send(event: VisualizationEvent, async: Boolean = true): Unit = {
    val json = compact(Extraction.decompose(event))

    async match {
      case true =>  Future { sendJson(json) }
      case false => sendJson(json)
    }
  }


  private def sendJson( json: String): Unit = {
    val client: Client[DefaultOptions, DefaultOptionsBuilder, DefaultRequestBuilder] = ClientFactory.getDefault.newClient.asInstanceOf[Client[DefaultOptions, DefaultOptionsBuilder, DefaultRequestBuilder]]

    val req = client.newRequestBuilder
      .method(Request.METHOD.GET)
      .uri(url)
      .transport(Request.TRANSPORT.WEBSOCKET)

    val opts = client.newOptionsBuilder().reconnect(false).build()
    val socket = client.create(opts).open(req.build())
    socket.fire(json)
    socket.close()
  }

  private def clean(str: String) = str.replaceAll("[^a-zA-Z0-9/]" , "");
}