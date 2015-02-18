package sparkly.component.classifier

import sparkly.testing._

class PerceptronSpec extends ClassifierSpec with SpamData {

  "Perceptron" should "learn spam data" in {
    val error = eval(classOf[Perceptron])
    error should be < 0.20
  }

}
