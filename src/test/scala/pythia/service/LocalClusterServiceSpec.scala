package pythia.service

import org.mockito.Mockito._
import org.scalatest._
import org.scalatest.mock.MockitoSugar
import pythia.component.CsvSource
import pythia.core._
import pythia.dao._
import pythia.service.ClusterState._
import pythia.testing.SpamData
import pythia.testing.component.DebugComponent

class LocalClusterServiceSpec  extends FlatSpec with Matchers with MockitoSugar with SpamData with BeforeAndAfterEach with BeforeAndAfterAll {

  implicit val pipelineRepository = mock[PipelineRepository]
  val localClusterService: LocalClusterService = new LocalClusterService()

  override def afterEach(): Unit = {
    localClusterService.stop(false)
  }

  override def afterAll(): Unit = {
    localClusterService.sparkContext.stop()
  }

  "Local cluster" should "deploy pipeline" in {
    // Given
    when(pipelineRepository.get("pipeline1")).thenReturn(Some(pipeline))

    // When
    localClusterService.deploy("pipeline1", false)

    // Then
    val state = localClusterService.status
    state.action should be (Running)
    state.time should not be empty
    state.pipeline should be (Some(pipeline))
    localClusterService.streamingContext should not be empty
  }

  "Local cluster" should "redeploy pipeline" in {
    // Given
    when(pipelineRepository.get("pipeline1")).thenReturn(Some(pipeline))
    localClusterService.deploy("pipeline1", false)

    // When
    localClusterService.deploy("pipeline1", false)

    // Then
    val state = localClusterService.status
    state.action should be (Running)
    state.time should not be empty
    state.pipeline should be (Some(pipeline))
    localClusterService.streamingContext should not be empty
  }

  val pipeline = PipelineConfiguration (
    name = "test",
    components = List (
      ComponentConfiguration (
        id = "csv_source",
        name = "Train data",
        clazz = classOf[CsvSource].getName,
        properties = Map(
          "File" -> "src/test/resources/spam.data"
        ),
        outputs = Map(
          "Instances" -> StreamConfiguration(selectedFeatures = Map("Features" -> (labelName :: featureNames)))
        )
      ),
      ComponentConfiguration (
        id = "debug",
        name = "Debug",
        clazz = classOf[DebugComponent].getName,
        inputs = Map(
          "Input" -> StreamConfiguration(selectedFeatures = Map("Features" -> List(labelName)))
        )
      )
    ),
    connections = List(
      ConnectionConfiguration("csv_source", "Instances", "debug", "Input")
    )
  )
}

