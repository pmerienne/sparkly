package pythia.component.preprocess

import org.apache.spark.streaming.dstream.DStream
import pythia.core.FeatureType.NUMBER
import pythia.core._
import pythia.core.PropertyType._
import pythia.core.OutputStreamMetadata
import pythia.core.InputStreamMetadata
import pythia.core.Context
import scala.Some
import pythia.core.PropertyMetadata

class Normalizer extends Component {

  def metadata = ComponentMetadata(
    name = "Normalizer", category = "Pre-processor",
    inputs = Map (
      "Input" -> InputStreamMetadata(listedFeatures = Map("Features" -> NUMBER))
    ),
    outputs = Map (
      "Output" -> OutputStreamMetadata(from = Some("Input"))
    ),
    properties = Map("Parallelism" -> PropertyMetadata(INTEGER, defaultValue = Some(-1), description = "Level of parallelism to use. -1 to use default level."))
  )

  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val parallelism = context.property("Parallelism").or(context.sc.defaultParallelism, on = (parallelism: Int) => parallelism < 1)
    Map("Output" -> context.dstream("Input", "Output").repartition(parallelism).map(instance => normalize(instance)))
  }

  def normalize(instance: Instance): Instance = {
    val originalFeatures = instance.inputFeatures("Features").as[Double]
    val magnitude = math.sqrt(originalFeatures.map(feature => feature * feature).reduce(_+_))

    val updatedFeatures = originalFeatures.map(feature => feature / magnitude)

    instance.inputFeatures("Features", updatedFeatures)
  }
}
