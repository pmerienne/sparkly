package sparkly.component.preprocess

import breeze.linalg._
import org.apache.spark.streaming.dstream.DStream
import sparkly.core.FeatureType.NUMBER
import sparkly.core.PropertyType._
import sparkly.core._

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
    val originalFeatures = instance.inputFeatures("Features").asDenseVector
    val squareSum = sum(originalFeatures.map(x => x * x))
    val magnitude = math.sqrt(squareSum)
    val updatedFeatures = originalFeatures.map(_ / magnitude)
    instance.inputFeatures("Features", updatedFeatures.toArray)
  }
}
