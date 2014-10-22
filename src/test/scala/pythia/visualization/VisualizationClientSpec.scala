package pythia.visualization

import org.scalatra.test.scalatest._
import org.scalatest._
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Span}
import pythia.web.resource.VisualizationResource
import pythia.core.{VisualizationEvent, VisualizationClient}
import scala.util.Random
import org.jfarcand.wcs.{TextListener, WebSocket}
import scala.collection.mutable.ListBuffer

class VisualizationClientSpec extends ScalatraFlatSpec with Matchers with Eventually {

  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(10, org.scalatest.time.Seconds)), interval = scaled(Span(100, Millis)))

  addServlet(classOf[VisualizationResource], "/api/visualization/*")

  val master = "master"
  val id = "test" + Random.nextInt(1000)

  ignore should "stream json data" in {
    // Given
    val (visualisationHost: String, visualisationPort: Int) = server.getConnectors
      .map(connector => (Option(connector.getHost), connector.getLocalPort))
      .filter{case (host, port) => host.isDefined && port > 0}
      .collectFirst{case (host, port) => (host.get, port)}
      .get

    val visualisationClient = new VisualizationClient(visualisationHost, visualisationPort, master, id)
    val messages = ListBuffer[String]()
    val ws = WebSocket().open(visualisationClient.url).listener(new TextListener{
      override def onMessage(msg: String) = messages += msg
    })

    //Then
    val event = VisualizationEvent(0L, Map("test" -> 1.0))
    visualisationClient.send(event, false)

    eventually {
      messages contains only (
        """{"timestampMs":0,"data":{"test":1.0}}"""
      )
    }

    ws.close
  }
}
