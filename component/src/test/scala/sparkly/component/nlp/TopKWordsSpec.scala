package sparkly.component.nlp

import sparkly.testing._
import sparkly.core._

class TopKWordsSpec  extends ComponentSpec {

  "TopKWords" should "get top k words from words" in {
    // Given
    val configuration = ComponentConfiguration(
      clazz = classOf[TopKWords].getName,
      name = "TopKWords",
      inputs = Map("Input" -> StreamConfiguration(mappedFeatures = Map("Text" -> "Text"))),
      outputs = Map("Output" -> StreamConfiguration(mappedFeatures = Map("Word" -> "Word", "Rank" -> "Rank", "Count" -> "Count"))),
      properties = Map (
        "K" -> "2",
        "Tokenize" -> "false"
      )
    )

    // When
    val component = deployComponent(configuration)
    component.inputs("Input").push(
      Instance("Text" -> "cat"),
      Instance("Text" -> "mice"),
      Instance("Text" -> "cat"),
      Instance("Text" -> "star"),
      Instance("Text" -> "star"),
      Instance("Text" -> "cat"),
      Instance("Text" -> "cat")
    )

    // Then
    eventually {
      component.outputs("Output").features should contain only (
        Map("Word" -> "cat", "Rank" -> 0, "Count" -> 4L),
        Map("Word" -> "star", "Rank" -> 1, "Count" -> 2L)
        )
    }
  }

  "TopKWords" should "get top k words from text with tokenizer" in {
    // Given
    val configuration = ComponentConfiguration(
      clazz = classOf[TopKWords].getName,
      name = "TopKWords",
      inputs = Map("Input" -> StreamConfiguration(mappedFeatures = Map("Text" -> "Text"))),
      outputs = Map("Output" -> StreamConfiguration(mappedFeatures = Map("Word" -> "Word", "Rank" -> "Rank", "Count" -> "Count"))),
      properties = Map (
        "K" -> "2",
        "Tokenize" -> "true",
        "Tokenizer language" -> "English"
      )
    )

    // When
    val component = deployComponent(configuration)
    component.inputs("Input").push(
      Instance("Text" -> "This cat is so nice"),
      Instance("Text" -> "To cat or not to cat, that's the question"),
      Instance("Text" -> "It is not in the stars to hold our destiny but in ourselves"),
      Instance("Text" -> "Is your cat an internet star?")
    )

    // Then
    eventually {
      component.outputs("Output").features should contain only (
        Map("Word" -> "cat", "Rank" -> 0, "Count" -> 4L),
        Map("Word" -> "star", "Rank" -> 1, "Count" -> 2L)
        )
    }
  }
}
