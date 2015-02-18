package pythia.component.classifier

import pythia.testing._

class PerceptronSpec extends ClassifierSpec with SpamData {

  "Perceptron" should "learn spam data" in {
    val error = eval(classOf[Perceptron])
    error should be < 0.20
  }

}
