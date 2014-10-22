package pythia.core

import org.jfarcand.wcs._
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.concurrent.Future
import java.net.{URLEncoder, URL}

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
    WebSocket().open(url).send(json)
  }

  private def clean(str: String) = URLEncoder.encode(str, "UTF-8")
}