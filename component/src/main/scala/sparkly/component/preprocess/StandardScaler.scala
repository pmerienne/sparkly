package sparkly.component.preprocess

import org.apache.spark.streaming.dstream.DStream
import sparkly.core.FeatureType.CONTINUOUS
import sparkly.core._
import sparkly.math._
import scala.util.Try

class StandardScaler extends Component {

  def metadata = ComponentMetadata(
    name = "Standard scaler", category = "Pre-processor",
    description = "Standardize continuous features by removing the mean and scaling to unit variance",
    inputs = Map (
      "Input" -> InputStreamMetadata(listedFeatures = Map("Features" -> CONTINUOUS))
    ),
    outputs = Map (
      "Output" -> OutputStreamMetadata(from = Some("Input"))
    )
  )

  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val size = context.inputSize("Input", "Features")
    var standardizer = Standardizer(size)

    context.dstream("Input", "Output")
      .map(i => i.inputFeatures("Features").asDenseVector)
      .mapPartitions(data => Iterator(Standardizer(size, data)))
      .reduce(_ + _)
      .foreachRDD{ rdd =>
        Try(rdd.take(1)(0)).foreach{ update =>
          standardizer = standardizer + update
        }
      }

    val out = context.dstream("Input", "Output").map{ instance =>
      val features = standardizer.standardize(instance.inputFeatures("Features").asDenseVector)
      instance.inputFeatures("Features", features.toArray)
    }

    Map("Output" -> out)
  }

}