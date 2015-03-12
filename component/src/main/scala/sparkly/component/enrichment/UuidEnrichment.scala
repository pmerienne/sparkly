package sparkly.component.enrichment

import sparkly.core._
import org.apache.spark.streaming.dstream.DStream
import java.util.Date
import sparkly.component.common.UUIDGen

class UuidEnrichment  extends Component {

  override def metadata: ComponentMetadata = ComponentMetadata (
    name = "UUID",  category = "Data Enrichments",
    description = "Add uuid feature",
    inputs = Map("In" -> InputStreamMetadata(namedFeatures = Map("Date" -> FeatureType.DATE))),
    outputs = Map("Out" -> OutputStreamMetadata(from = Some("In"), namedFeatures = Map ("UUID" -> FeatureType.STRING)))
  )

  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val out = context.dstream("In", "Out").map{ instance =>
      val date = instance.inputFeature("Date").or(new Date())
      val uuid = UUIDGen.getTimeUUID(date.getTime).toString
      instance.outputFeature("UUID", uuid)
    }

    Map("Out" -> out)
  }

}
