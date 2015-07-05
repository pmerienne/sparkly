package sparkly.component.preprocess

import org.apache.spark.streaming.dstream.DStream
import sparkly.core.FeatureType.{CATEGORICAL, CONTINUOUS}
import sparkly.core.PropertyType.INTEGER
import sparkly.core._
import org.apache.spark.Logging

class CategoricalEncoder extends Component with Logging {

  def metadata = ComponentMetadata (
    name = "Categorical encoder", category = "Pre-processor",
    description =
      """
        |Encode categorical features using One-hot/One-of-k encoding scheme.
        |It will transform a categorical feature into 'k' binary features (0.0 and 1.0), with only one active.
        |'k' is the number of distinct categories and it's specified in the output configurations.
      """.stripMargin,
    inputs = Map (
      "Input" -> InputStreamMetadata(namedFeatures = Map("Feature" -> CATEGORICAL))
    ),
    outputs = Map (
      "Output" -> OutputStreamMetadata(from = Some("Input"), listedFeatures = Map("Encoded features" -> CONTINUOUS))
    ),
    properties = Map(
      "Parallelism" -> PropertyMetadata(INTEGER, defaultValue = Some(-1), description = "Level of parallelism to use. -1 to use default level.")
    )
  )

  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val n = context.outputSize("Output", "Encoded features")
    val partitions = context.property("Parallelism").as[Int] match {
      case positive if positive > 0 => positive
      case _ => context.sc.defaultParallelism
    }

    val stream = context.dstream("Input", "Output")

    var categoryIndex = CategoryIndex()
    stream.map(i => i.inputFeature("Feature").asString).foreachRDD{ rdd =>
      categoryIndex = categoryIndex.add(rdd.distinct(partitions).collect())
    }

    val out = stream.map{ instance =>
      val encoded = Array.fill(n)(0.0)

      val index = categoryIndex(instance.inputFeature("Feature").asString).toInt
      if(index < n) {
        encoded(index.toInt) = 1.0
      } else {
        logError(s"Got $index categories but encoder is limited to $n categories")
      }

      instance.outputFeatures("Encoded features", encoded)
    }

    Map("Output" -> out)
  }

}

case class CategoryIndex(index: Map[String, Long] = Map()) {
  def apply(category: String) = index(category)

  def add(category: String): CategoryIndex = index.contains(category) match {
    case true => this
    case false => this.copy(index = index + (category -> index.size))
  }

  def add(categories: Iterable[String]): CategoryIndex = categories.foldLeft(this)((current, category) => current.add(category))
}