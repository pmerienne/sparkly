package sparkly.visualization

import org.scalatest.concurrent.Eventually
import org.scalatest._
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import java.nio.file.Files
import sparkly.core._
import org.scalatest.mock.MockitoSugar
import org.mockito.{Mockito, ArgumentCaptor}
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.scalatest.time.{Millis, Span}
import org.apache.spark.streaming.dstream.DStream
import sparkly.testing._
import scala.collection.mutable.{Map => MutableMap}
import scala.reflect.io.Directory
import sparkly.utils.SparklyDirectoryStructure

// TODO : this class should use StreamingContextFactory
// We need to spawn a web socket server to do so (avoid using mock)
class VisualizationSpec extends FlatSpec with Matchers with Eventually with BeforeAndAfterEach  with MockitoSugar {

  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(10, org.scalatest.time.Seconds)), interval = scaled(Span(100, Millis)))

  val pipelineId = "visualization-spec"
  val baseDirectory = Directory.makeTemp("visualization-spec")
  val pipelineDirectory = SparklyDirectoryStructure.getPipelineDirectory(baseDirectory.toString, pipelineId)
  val checkpointDirectory = SparklyDirectoryStructure.getCheckpointDirectory(pipelineDirectory, pipelineId)

  val visualizationBuilder = new VisualizationBuilder("localhost", 8080)
  val dataCollector = mock[VisualizationDataCollector](withSettings().serializable())

  var ssc: StreamingContext = null

  override def beforeEach() {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test-" + this.getClass.getSimpleName)

    ssc = new StreamingContext(conf, Milliseconds(50))
    ssc.checkpoint(checkpointDirectory.toString)
  }

  override def afterEach() {
    ssc.stop()
    baseDirectory.deleteRecursively()
  }

  def deployVisualization(configuration: VisualizationConfiguration): BuiltVisualization = {
    val outputStreams = MutableMap[(String, String), DStream[Instance]]()

    val streams = configuration.streams.map{case (name, streamIdentifier) =>
      val config: ComponentConfiguration = ComponentConfiguration(id = streamIdentifier.component, name = "visualization stream", clazz = classOf[MockStream].getName)
      val mockStream = classOf[MockStream].newInstance()
      val outputs = mockStream.init(ssc, pipelineDirectory, pipelineId, config, Map())
      outputStreams += (config.id, streamIdentifier.stream) -> outputs(MockStream.OUTPUT_NAME)
      (streamIdentifier, mockStream)
    }

    val features = configuration.features.map{case (name, featureIdentifier) =>
      val config: ComponentConfiguration = ComponentConfiguration(id = featureIdentifier.component, name = "visualization stream", clazz = classOf[MockStream].getName)
      val mockStream = classOf[MockStream].newInstance()
      val outputs = mockStream.init(ssc, pipelineDirectory, pipelineId, config, Map())
      outputStreams += (config.id, featureIdentifier.stream) -> outputs(MockStream.OUTPUT_NAME)
      (featureIdentifier, mockStream)
    }

    val visualization = Class.forName(configuration.clazz).newInstance.asInstanceOf[Visualization]
    val context = visualizationBuilder.buildContext(ssc, dataCollector, visualization.metadata, configuration, outputStreams.toMap)
    visualization.init(context)

    ssc.start()
    BuiltVisualization(streams, features)
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

case class BuiltVisualization(streams: Map[StreamIdentifier, MockStream], features: Map[FeatureIdentifier, MockStream]) {
  def mockStream(component: String, stream: String) = streams(StreamIdentifier(component, stream))
  def mockStream(component: String, stream: String, feature: String) = features(FeatureIdentifier(component, stream, feature))
}