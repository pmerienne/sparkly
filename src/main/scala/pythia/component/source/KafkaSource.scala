package pythia.component.source

import pythia.core._
import pythia.core.PropertyType._
import pythia.core.OutputStreamMetadata
import pythia.core.ComponentMetadata
import pythia.core.PropertyMetadata
import scala.Some
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.storage.StorageLevel

class KafkaSource extends Component {

  override def metadata = ComponentMetadata (
    name = "Kafka stream", description = "Reads messages from a topic in Kafka.",
    category = "Data Sources",
    outputs = Map(
      "Output" -> OutputStreamMetadata(namedFeatures = Map("Message" -> FeatureType.STRING))
    ),
    properties = Map(
      "Topic" -> PropertyMetadata(STRING),
      "Group Id" -> PropertyMetadata(STRING),
      "Consumers threads" -> PropertyMetadata(INTEGER, defaultValue = Some(1)),
      "Zookeeper quorum" -> PropertyMetadata(STRING, defaultValue = Some(";"), description = "Format hostname:port,hostname:port,...")
    )
  )

  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {

    val inputTopic = context.property("Topic").as[String]
    val group = context.property("Group Id").as[String]
    val consumers = context.property("Consumers threads").as[Int]
    val zkQuorum = context.property("Zookeeper quorum").as[String]

    val featureName = context.outputFeatureName("Output", "Message")

    val streams = (1 to consumers) map { _ =>
      KafkaUtils.createStream(context.ssc, zkQuorum, group, Map(inputTopic -> 1), StorageLevel.MEMORY_AND_DISK_SER).map(_._2)
    }

    val unifiedStream = context.ssc.union(streams)

    Map("Output" -> unifiedStream.map(message => Instance(featureName -> message)))
  }
}
