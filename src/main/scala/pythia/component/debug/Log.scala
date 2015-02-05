package pythia.component.debug

import org.apache.spark.streaming.dstream.DStream
import pythia.core.FeatureType.ANY
import pythia.core._
import pythia.core.PropertyType._

class Log extends Component {

  override def metadata: ComponentMetadata = ComponentMetadata (
    name = "Log",
    inputs = Map[String, InputStreamMetadata] (
      "Input" -> InputStreamMetadata(listedFeatures = Map("Features" -> ANY))
    )
  )

  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val features = context.inputMappers("Input").featuresNames("Features")
    context
      .dstream("Input")
      .map(instance => instance.rawFeatures.filterKeys(features.contains).toString)
      .print()

    Map()
  }
}
