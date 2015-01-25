package pythia.service

import org.mockito.Mockito._
import org.scalatest._
import org.scalatest.mock.MockitoSugar
import pythia.component.source.CsvFileDirectorySource
import pythia.core._
import pythia.dao._
import pythia.service.ClusterState._
import pythia.config.PythiaConfig._
import pythia.testing.SpamData
import pythia.component.debug.DebugComponent
import scala.reflect.io.Directory

class LocalClusterServiceSpec extends FlatSpec with Matchers with MockitoSugar with SpamData with BeforeAndAfterEach {

  implicit val pipelineRepository = mock[PipelineRepository]
  implicit val pipelineValidationService = mock[PipelineValidationService]
  val localClusterService: LocalClusterService = new LocalClusterService()

  override def beforeEach(): Unit = {
    Directory(BASE_CHECKPOINTS_DIRECTORY).deleteRecursively()
  }

  override def afterEach(): Unit = {
    localClusterService.stop(false)
    Directory(BASE_CHECKPOINTS_DIRECTORY).deleteRecursively()
  }

  "Local cluster" should "deploy pipeline" in {
    // Given
    when(pipelineRepository.get("pipeline1")).thenReturn(Some(pipeline))
    when(pipelineValidationService.validate(pipeline)).thenReturn(ValidationReport())

    // When
    localClusterService.deploy("pipeline1", false)

    // Then
    val state = localClusterService.status
    state.action should be (Running)
    state.time should not be empty
    state.pipeline should be (Some(pipeline))
    localClusterService.streamingContext should not be empty
  }

  "Local cluster" should "not deploy pipeline when not valid" in {
    // Given
    when(pipelineRepository.get("pipeline1")).thenReturn(Some(pipeline))
    when(pipelineValidationService.validate(pipeline)).thenReturn(ValidationReport(List(ValidationMessage("", MessageLevel.Error))))

    // When
    intercept[IllegalArgumentException] {
      localClusterService.deploy("pipeline1", false)
    }

    // Then
    val state = localClusterService.status
    state.action should be (Stopped)
  }

  "Local cluster" should "redeploy pipeline" in {
    // Given
    when(pipelineRepository.get("pipeline1")).thenReturn(Some(pipeline))
    when(pipelineValidationService.validate(pipeline)).thenReturn(ValidationReport())
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
        clazz = classOf[CsvFileDirectorySource].getName,
        properties = Map(
          "Directory" -> "src/test/resources",
          "Process only new files" -> "false",
          "Filename pattern" -> "spam.data"
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

