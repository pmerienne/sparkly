package pythia.testing

import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.scalatest._
import org.scalatest.concurrent.Eventually
import org.scalatest.time._
import pythia.core._

import scala.reflect.io.Directory
import scala.util.Try

trait ComponentSpec extends FlatSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll with Eventually {

  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(10, org.scalatest.time.Seconds)), interval = scaled(Span(100, Millis)))

  var pipelineDirectory = Directory.makeTemp("sparkly-component-test")
  val streamingContextFactory = new StreamingContextFactory(pipelineDirectory.toString(), "local[8]", "test-cluster", Milliseconds(200), "localhost", 8080)

  var ssc: Option[StreamingContext] = None

  override def beforeEach() {
    super.beforeEach()
    pipelineDirectory.deleteRecursively()
  }

  override def afterEach() {
    super.afterEach()
    ssc.foreach{ssc =>
      ssc.stop()
      ssc.awaitTermination(2000)
    }
    pipelineDirectory.deleteRecursively()
  }

  def deployComponent(componentConfiguration: ComponentConfiguration): RunningComponent = {
    val inputComponents = inputMockStream(componentConfiguration)
    val connections = inputComponents.map(inputComponent => ConnectionConfiguration(inputComponent._2.id, MockStream.OUTPUT_NAME, componentConfiguration.id, inputComponent._1)).toList
    val components = componentConfiguration :: inputComponents.values.toList

    val pipeline = PipelineConfiguration(name = this.getClass.getSimpleName, components = components, connections = connections)
    val (streamingContext, buildResult) = streamingContextFactory.createStreamingContext(pipeline)

    val mockedInputs = inputComponents.map{case(name, config) => // TODO ugly!
      val mockInput = buildResult.components.values.find(component => Try(component.asInstanceOf[MockStream].componentId == config.id).getOrElse(false))
      (name, mockInput.get.asInstanceOf[MockStream])
    }

    val inspectedOutputs = buildResult.outputs.map{case (names, dstream) => (names._2, InspectedStream(dstream))}

    ssc = Some(streamingContext)
    ssc.get.start()

    RunningComponent(mockedInputs, inspectedOutputs)
  }

  private def inputMockStream(component: ComponentConfiguration) = {
    ComponentMetadata.of(component).inputs.keys.map(name => (name, ComponentConfiguration(name = name, clazz = classOf[MockStream].getName))).toMap
  }

}

case class RunningComponent(inputs: Map[String, MockStream], outputs: Map[String, InspectedStream])
