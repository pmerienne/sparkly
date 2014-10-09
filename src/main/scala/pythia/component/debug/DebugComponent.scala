package pythia.component.debug

import org.apache.spark.streaming.dstream.DStream
import pythia.core._

class DebugComponent extends Component {

  override def metadata: ComponentMetadata = ComponentMetadata (
    name = "Debug",
    inputs = Map[String, InputStreamMetadata] (
      "Input" -> InputStreamMetadata(listedFeatures = List("Features"))
    ),
    properties = Map (
      "Log component name (fake)" -> PropertyMetadata("BOOLEAN", defaultValue = Some(true))
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
