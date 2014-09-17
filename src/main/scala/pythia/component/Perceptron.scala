package pythia.component

import pythia.core._

class Perceptron extends Component {
  def metadata = Metadata (
    "Perceptron", "Linear classifier. See http://en.wikipedia.org/wiki/Perceptron",
    properties = List(PropertyMetadata("Bias", "DECIMAL", "0.1"), PropertyMetadata("Learning rate", "DECIMAL", "0.01")),
    inputStreams = List(
      StreamMetadata("Train", listedFeatures = List("Features"), namedFeatures = List("Label")),
      StreamMetadata("Query", listedFeatures = List("Features"))
    ),
    outputStreams = List(
      StreamMetadata("Query result",from = "Query", namedFeatures = List("Label"))
    )
  )
}
