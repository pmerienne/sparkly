package pythia.visualization

import org.scalatest.concurrent.Eventually
import org.scalatest._
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import java.nio.file.Files
import pythia.core._
import org.scalatest.mock.MockitoSugar
import org.mockito.{Mockito, ArgumentCaptor}
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.scalatest.time.{Millis, Span}
import pythia.testing.MockStream

class VisualizationSpec extends FlatSpec with Matchers with Eventually with BeforeAndAfterEach  with MockitoSugar {

  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(10, org.scalatest.time.Seconds)), interval = scaled(Span(100, Millis)))

  val visualizationClient = mock[VisualizationClient]

  var ssc: StreamingContext = null
  var stream: MockStream = null


  override def beforeEach() {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test-" + this.getClass.getSimpleName)

    ssc = new StreamingContext(conf, Milliseconds(50))
    ssc.checkpoint(Files.createTempDirectory("pythia-test").toString)

    stream = MockStream(ssc)
    stream.dstream.foreachRDD{rdd => val doNothing = true}// Do nothing
  }

  override def afterEach() {
    ssc.stop()
  }

  def launchVisualization[T <: Visualization](vizClazz: Class[T]): Unit = {
    val visualization = vizClazz.newInstance
    visualization.init(stream.dstream, ssc, visualizationClient)

    ssc.start()
  }

  def allSentOutData() = {
    val captor = ArgumentCaptor.forClass(classOf[Map[String, Double]])
    verify(visualizationClient, Mockito.atLeast(0)).send(any[Long], captor.capture())
    captor.getAllValues
  }
  def latestSentOutData() = {
    val captor = ArgumentCaptor.forClass(classOf[Map[String, Double]])
    verify(visualizationClient, Mockito.atLeast(0)).send(any[Long], captor.capture())
    captor.getValue
  }
}
