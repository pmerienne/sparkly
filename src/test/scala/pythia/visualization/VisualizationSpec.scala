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
import org.apache.spark.streaming.dstream.DStream

class VisualizationSpec extends FlatSpec with Matchers with Eventually with BeforeAndAfterEach  with MockitoSugar {

  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(10, org.scalatest.time.Seconds)), interval = scaled(Span(100, Millis)))

  val visualizationBuilder = new VisualizationBuilder("localhost", 8080)
  val dataCollector = mock[VisualizationDataCollector](withSettings().serializable())

  var ssc: StreamingContext = null
  var outputStreams: Map[(String, String), DStream[Instance]] = Map()

  override def beforeEach() {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test-" + this.getClass.getSimpleName)

    ssc = new StreamingContext(conf, Milliseconds(50))
    ssc.checkpoint(Files.createTempDirectory("pythia-test").toString)
  }

  override def afterEach() {
    ssc.stop()
  }

  def launchVisualization(configuration: VisualizationConfiguration): Unit = {
    val visualization = Class.forName(configuration.clazz).newInstance.asInstanceOf[Visualization]
    val context = visualizationBuilder.buildContext(ssc, dataCollector, visualization.metadata, configuration, outputStreams)
    visualization.init(context)

    ssc.start()
  }

  def allSentOutData() = {
    val captor = ArgumentCaptor.forClass(classOf[Map[String, Double]])
    verify(dataCollector, Mockito.atLeast(0)).push(any[Long], captor.capture())
    captor.getAllValues
  }
  def latestSentOutData() = {
    val captor = ArgumentCaptor.forClass(classOf[Map[String, Double]])
    verify(dataCollector, Mockito.atLeast(0)).push(any[Long], captor.capture())
    captor.getValue
  }
}
