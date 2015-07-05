package sparkly.component.source.dataset

import scala.Some
import sparkly.core._
import org.apache.spark.streaming.dstream.DStream
import sparkly.core.FeatureType.FeatureType
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import scala.io.Source
import com.google.common.util.concurrent.RateLimiter

abstract class DatasetSource extends Component {

  def features: List[(String, FeatureType)]
  def file: String

  def metadata = ComponentMetadata (
    name = "Dataset source", category = "Dataset",
    outputs = Map("Instances" -> OutputStreamMetadata(namedFeatures = features.toMap)),
    properties = Map(
      "Throughput (instance/second)" -> PropertyMetadata(PropertyType.INTEGER, defaultValue = Some(1000)),
      "Loop" -> PropertyMetadata(PropertyType.BOOLEAN, defaultValue = Some(false))
    )
  )

  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val throughput = context.property("Throughput (instance/second)").as[Int]
    val loop = context.property("Loop").as[Boolean]
    val stream: DStream[Instance] = context.ssc.receiverStream(new DatasetReceiver(file, features, throughput, loop))
    Map("Instances" -> stream)
  }

}

class DatasetReceiver(file: String, features: List[(String, FeatureType)], throughput: Int, loop: Boolean) extends Receiver[Instance](StorageLevel.MEMORY_ONLY) with Logging {


  def onStart() {
    new Thread(s"$file dataset thread") {
      override def run() {
        val limiter = RateLimiter.create(throughput)
        var it = createDatasetIterator()

        while(!isStopped) {
          limiter.acquire()

          if(loop && it.isEmpty) {
            it = createDatasetIterator()
          }

          if(it.hasNext) {
            store(it.next())
          }
        }
      }
    }.start()
  }

  def onStop() {
  }

  private def createDatasetIterator(): Iterator[Instance] = {
    Source
      .fromInputStream(getClass.getResourceAsStream(file))
      .getLines()
      .map{ line =>
        val raw = (features.map(_._1), line.split(",").toList).zipped
        val values = convert(raw.toMap)
        Instance(values)
      }
  }

  private def convert(raw: Map[String, String]): Map[String, Any] = {
    val featureTypes = features.toMap
    raw.map{ case (name, str) =>
      val value = featureTypes(name) match {
        case FeatureType.CONTINUOUS | FeatureType.DOUBLE => str.toDouble
        case FeatureType.INTEGER => str.toInt
        case FeatureType.LONG => str.toLong
        case FeatureType.BOOLEAN => str match {
          case "true" | "1" => true
          case "false" | "0" | "-1" => false
        }
        case _ => str
      }
      (name, value)
    }
  }
}
