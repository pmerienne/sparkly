package sparkly.component.nlp

import sparkly.testing._
import sparkly.core._

class TwitterSentimentClassifierSpec extends ComponentSpec {

  "TwitterSentimentClassifier" should "predict sentiment from tweets" in {
    // Given
    val configuration = ComponentConfiguration (
      clazz = classOf[TwitterSentimentClassifier].getName,
      name = "TwitterSentimentClassifier",
      inputs = Map ("Input" -> StreamConfiguration(mappedFeatures = Map("Tweet" -> "Tweet"))),
      outputs = Map ("Output" -> StreamConfiguration(mappedFeatures = Map("Sentiment" -> "Sentiment")))
    )

    // When
    val component = deployComponent(configuration)
    component.inputs("Input").push(
      Instance("Tweet" -> "i love lebron. http://bit.ly/PdHur"),
      Instance("Tweet" -> "Recovering from surgery..wishing @julesrenner was here :(")
    )

    // Then
    eventually {
      component.outputs("Output").features should contain only (
        Map("Tweet" -> "i love lebron. http://bit.ly/PdHur", "Sentiment" -> 1.0),
        Map("Tweet" -> "Recovering from surgery..wishing @julesrenner was here :(", "Sentiment" -> 0.0)
      )
    }
  }

}
