package sparkly.service

import org.mockito.BDDMockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, FlatSpec}

import sparkly.core._
import sparkly.core.FeatureType._
import sparkly.core.PropertyType._
import sparkly.dao._
import sparkly.service._

class PipelineValidationServiceSpec extends FlatSpec with Matchers with MockitoSugar {

  implicit val componentRepository = mock[ComponentRepository]
  implicit val visualizationRepository = mock[VisualizationRepository]
  val componentValidator = new ComponentValidator()
  val visualizationValidator = new VisualizationValidator()

  "ComponentValidator" should "detect bad implementation class" in {
    // Given
    val configuration = ComponentConfiguration (name = "Test", clazz = "TestClass")
    given(componentRepository.component("TestClass")).willReturn(None)

    // When
    val messages = componentValidator.validate(configuration)

    // Then
    messages should contain (ValidationMessage("'Test' component implementation 'TestClass' is unknown.", MessageLevel.Error))
  }

  "ComponentValidator" should "detect missing property" in {
    // Given
    given(componentRepository.component("Test")).willReturn(Some(
      ComponentMetadata("Test",
        properties = Map (
          "Not mandatory" -> PropertyMetadata(DECIMAL, mandatory = false),
          "Mandatory" -> PropertyMetadata(DECIMAL, mandatory = true),
          "Mandatory 2" -> PropertyMetadata(DECIMAL, defaultValue = Some(0.25), mandatory = true)
        )
      )
    ))

    val configuration = ComponentConfiguration (name = "Test",
      clazz = "Test",
      properties = Map()
    )

    // When
    val messages = componentValidator.validate(configuration)

    // Then
    messages should contain only (ValidationMessage("'Mandatory' property of 'Test' component is mandatory.", MessageLevel.Error))
  }

  "ComponentValidator" should "detect bad property value" in {
    // Given
    given(componentRepository.component("TestClass")).willReturn(Some(
      ComponentMetadata("TestClass",
        properties = Map (
          "Bias 1" -> PropertyMetadata(DECIMAL),
          "Bias 2" -> PropertyMetadata(DECIMAL)
        )
      )
    ))

    val configuration = ComponentConfiguration (name = "Test",
      clazz = "TestClass",
      properties = Map("Bias 1" -> "0.25", "Bias 2" -> "flute")
    )

    // When
    val messages = componentValidator.validate(configuration)

    // Then
    messages should contain only (ValidationMessage("'Bias 2' property of 'Test' component is a DECIMAL property. Value 'flute' is not supported.", MessageLevel.Error))
  }

  "ComponentValidator" should "detect missing mappings" in {
    // Given
    given(componentRepository.component("TestClass")).willReturn(Some(
      ComponentMetadata("TestClass",
        inputs = Map (
          "Input" -> InputStreamMetadata(namedFeatures = Map("Input Feature 1" -> ANY, "Input Feature 2" -> ANY))
        ),
        outputs = Map (
          "Output" -> OutputStreamMetadata(namedFeatures = Map("Output Feature 1" -> ANY, "Output Feature 2" -> ANY))
        )
      )
    ))

    val configuration = ComponentConfiguration (name = "Test", clazz = "TestClass",
      inputs = Map (
        "Input" -> StreamConfiguration(mappedFeatures = Map("Input Feature 1" -> "f1"))
      ),
      outputs = Map (
        "Output" -> StreamConfiguration(mappedFeatures = Map("Output Feature 2" -> "f4"))
      )
    )

    // When
    val messages = componentValidator.validate(configuration)

    // Then
    messages should contain only (
      ValidationMessage("Missing mapping 'Input Feature 2' of input 'Input' in 'Test' component.", MessageLevel.Error),
      ValidationMessage("Missing mapping 'Output Feature 1' of output 'Output' in 'Test' component.", MessageLevel.Error)
    )
  }

  "VisualizationValidator" should "detect bad implementation class" in {
    // Given
    val configuration = VisualizationConfiguration(name = "Test", clazz = "ErrorClass")
    given(visualizationRepository.visualization("ErrorClass")).willReturn(None)

    // When
    val messages = visualizationValidator.validate(configuration)

    // Then
    messages should contain (ValidationMessage("'Test' visualization implementation 'ErrorClass' is unknown.", MessageLevel.Error))
  }

  "VisualizationValidator" should "detect missing property" in {
    // Given
    given(visualizationRepository.visualization("Test")).willReturn(Some(
      VisualizationMetadata("Test",
        properties = Map (
          "Not mandatory" -> PropertyMetadata(DECIMAL, mandatory = false),
          "Mandatory" -> PropertyMetadata(DECIMAL, mandatory = true),
          "Mandatory 2" -> PropertyMetadata(DECIMAL, defaultValue = Some(0.25), mandatory = true)
        )
      )
    ))

    val configuration = VisualizationConfiguration (name = "Test",
      clazz = "Test",
      properties = Map()
    )

    // When
    val messages = visualizationValidator.validate(configuration)

    // Then
    messages should contain only (ValidationMessage("'Mandatory' property of 'Test' visualization is mandatory.", MessageLevel.Error))
  }

  "VisualizationValidator" should "detect bad property value" in {
    // Given
    given(visualizationRepository.visualization("TestClass")).willReturn(Some (
      VisualizationMetadata("TestClass",
        properties = Map (
          "Bias 1" -> PropertyMetadata(DECIMAL),
          "Bias 2" -> PropertyMetadata(DECIMAL)
        )
      )
    ))

    val configuration = VisualizationConfiguration (name = "Test",
      clazz = "TestClass",
      properties = Map("Bias 1" -> "0.25", "Bias 2" -> "flute")
    )

    // When
    val messages = visualizationValidator.validate(configuration)

    // Then
    messages should contain only (ValidationMessage("'Bias 2' property of 'Test' visualization is a DECIMAL property. Value 'flute' is not supported.", MessageLevel.Error))
  }

  "VisualizationValidator" should "detect missing stream" in {
    // Given
    given(visualizationRepository.visualization("Test")).willReturn(Some(
      VisualizationMetadata("Test",
        streams = List("stream1", "stream2", "stream3", "stream4")
      )
    ))

    val configuration = VisualizationConfiguration (name = "Test",
      clazz = "Test",
      streams = Map (
        "stream1" -> StreamIdentifier("component", "stream"),
        "stream2" -> StreamIdentifier("component", null),
        "stream3" -> StreamIdentifier(null, "stream")
      )
    )

    // When
    val messages = visualizationValidator.validate(configuration)

    // Then
    messages should contain only (
      ValidationMessage("'stream2' stream of 'Test' visualization is missing.", MessageLevel.Error),
      ValidationMessage("'stream3' stream of 'Test' visualization is missing.", MessageLevel.Error),
      ValidationMessage("'stream4' stream of 'Test' visualization is missing.", MessageLevel.Error)
    )
  }

  "VisualizationValidator" should "detect missing feature" in {
    // Given
    given(visualizationRepository.visualization("Test")).willReturn(Some(
      VisualizationMetadata("Test",
        features = List("feature1", "feature2", "feature3", "feature4", "feature5")
      )
    ))

    val configuration = VisualizationConfiguration (name = "Test",
      clazz = "Test",
      features = Map (
        "feature1" -> FeatureIdentifier("component", "stream", "feature1"),
        "feature2" -> FeatureIdentifier("component", "stream", null),
        "feature3" -> FeatureIdentifier("component", null, "feature3"),
        "feature4" -> FeatureIdentifier(null, "stream", "feature4")
      )
    )

    // When
    val messages = visualizationValidator.validate(configuration)

    // Then
    messages should contain only (
      ValidationMessage("'feature2' feature of 'Test' visualization is missing.", MessageLevel.Error),
      ValidationMessage("'feature3' feature of 'Test' visualization is missing.", MessageLevel.Error),
      ValidationMessage("'feature4' feature of 'Test' visualization is missing.", MessageLevel.Error),
      ValidationMessage("'feature5' feature of 'Test' visualization is missing.", MessageLevel.Error)
    )
  }
}
