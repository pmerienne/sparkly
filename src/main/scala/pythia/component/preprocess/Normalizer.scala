package pythia.component.preprocess

import org.apache.spark.streaming.dstream.DStream
import pythia.core.FeatureType.NUMBER
import pythia.core._

class Normalizer extends Component {

  def metadata = ComponentMetadata(
    name = "Normalizer", category = "Pre-processor",
    inputs = Map (
      "Input" -> InputStreamMetadata(listedFeatures = Map("Features" -> NUMBER))
    ),
    outputs = Map (
      "Output" -> OutputStreamMetadata(from = Some("Input"))
    )
  )

  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    Map("Output" -> context.dstream("Input", "Output").map(instance => normalize(instance)))
  }

  def normalize(instance: Instance): Instance = {
    val originalFeatures = instance.inputFeatures("Features").as[Double]
    val magnitude = math.sqrt(originalFeatures.map(feature => feature * feature).reduce(_+_))

    val updatedFeatures = originalFeatures.map(feature => feature / magnitude)

    instance.inputFeatures("Features", updatedFeatures)
  }
}
