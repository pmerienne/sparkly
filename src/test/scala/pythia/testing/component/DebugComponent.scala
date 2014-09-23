package pythia.testing.component

import org.apache.spark.streaming.dstream.DStream
import pythia.core._

class DebugComponent extends Component {

  override def metadata: ComponentMetadata = ComponentMetadata (
    name = "Debug",
    inputs = Map[String, InputStreamMetadata] (
      "Input" -> InputStreamMetadata(listedFeatures = List("Features"))
    )
  )

  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    context
      .dstream("Input")
      .print()

    Map()
  }
}
