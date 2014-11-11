package pythia.core

import org.atmosphere.wasync.impl.{DefaultRequestBuilder, DefaultOptionsBuilder, DefaultOptions}
import org.atmosphere.wasync._
import org.scalatest.Matchers
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Span}
import org.scalatra.test.scalatest.ScalatraFlatSpec
import pythia.web.resource.VisualizationResource

import scala.collection.mutable.ListBuffer
import scala.util.Random

class VisualizationDataCollectorSpec extends ScalatraFlatSpec with Matchers with Eventually {

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

    val dataCollector = new VisualizationDataCollector(visualisationHost, visualisationPort, master, id)

    val messages = ListBuffer[String]()
    val client: Client[DefaultOptions, DefaultOptionsBuilder, DefaultRequestBuilder] = ClientFactory.getDefault.newClient.asInstanceOf[Client[DefaultOptions, DefaultOptionsBuilder, DefaultRequestBuilder]]
    val opts = client.newOptionsBuilder().reconnect(false).build()
    val req = client.newRequestBuilder
      .method(Request.METHOD.GET)
      .uri(dataCollector.url)
      .transport(Request.TRANSPORT.WEBSOCKET)
    val socket = client.create(opts).on(Event.MESSAGE, new Function[String] {
      def on(msg: String) = messages += msg
    })
    socket.open(req.build())

    //Then
    dataCollector.push(0L, Map("test" -> 1.0), false)

    eventually {
      messages contains only (
        """{"timestampMs":0,"data":{"test":1.0}}"""
      )
    }

    socket.close
  }

}
