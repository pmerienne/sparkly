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
      "Log component name" -> PropertyMetadata("BOOLEAN", defaultValue = Some(true))
    )
  )

  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    context
      .dstream("Input")
      .print()

    Map()
  }
}
