package sparkly.component.preprocess

import org.apache.spark.streaming.dstream.DStream
import sparkly.core.FeatureType.VECTOR
import sparkly.core._
import sparkly.math._
import scala.util.Try

class StandardScaler extends Component {

  def metadata = ComponentMetadata(
    name = "Standard scaler", category = "Pre-processor",
    description = "Standardize continuous features by removing the mean and scaling to unit variance",
    inputs = Map (
      "Input" -> InputStreamMetadata(namedFeatures = Map("Features" -> VECTOR))
    ),
    outputs = Map (
      "Output" -> OutputStreamMetadata(from = Some("Input"), namedFeatures = Map("Standardized features" -> VECTOR))
    )
  )

  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    var size: Int = -1
    var standardizer: Standardizer = null

    val features = context.dstream("Input", "Output").map(i => i.inputFeature("Features").asVector)

    features.foreachRDD{ rdd => if(standardizer == null && !rdd.isEmpty) {
      size = rdd.first().length
      standardizer = Standardizer(size)
    }}

    features
      .mapPartitions( data => if(data.isEmpty) Iterator() else Iterator(Standardizer(size, data)))
      .reduce(_ + _)
      .foreachRDD{ rdd =>
        Try(rdd.take(1)(0)).foreach{ update =>
          standardizer = standardizer + update
        }
      }

    val out = context.dstream("Input", "Output").map{ instance =>
      val features = standardizer.standardize(instance.inputFeature("Features").asVector)
      instance.outputFeature("Standardized features", features)
    }

    Map("Output" -> out)
  }

}