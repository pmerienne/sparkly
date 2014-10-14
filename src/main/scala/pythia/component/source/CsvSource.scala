package pythia.component.source

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_2
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.receiver.Receiver

import pythia.core._
import pythia.core.PropertyType._

import scala.collection.mutable
import scala.io.Source

class CsvSource extends Component {

  override def metadata =  ComponentMetadata (
    name = "Csv source", description = "Read and parse a given csv file", category = "Source",
    outputs = Map (
      "Instances" -> OutputStreamMetadata(listedFeatures = Map("Features" -> FeatureType.STRING))
    ),
    properties = Map (
      "File" -> PropertyMetadata(STRING),
      "Separator" -> PropertyMetadata(STRING, defaultValue = Some(";"), acceptedValues = List(";", ","))
    )
  )

  override def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val file = context.properties("File").as[String]
    val separator = context.properties("Separator").as[String]

    val featureNames = context.outputMappers("Instances").featuresNames("Features")

    val queue = new mutable.SynchronizedQueue[RDD[String]]()
    val fileStream = context.ssc.queueStream(queue)
    val stream = fileStream
      .map(line => line.split(separator).toList)
      .map(values => {
        val entries = values.zipWithIndex
          .filter { case (value, index) => index < featureNames.size}
          .map { case (value, index) => featureNames(index) -> value}
          .toMap

        Instance(entries)
      })

    val lines = Source.fromFile(file).getLines().toList
    queue += context.ssc.sparkContext.makeRDD(lines)

    Map("Instances" -> stream)
  }
}

class CustomReceiver(filename: String) extends Receiver[String](MEMORY_AND_DISK_2) with Logging {

  def onStart() {
    Source.fromFile(filename).getLines().foreach(line => store(line))
  }

  def onStop() {
  }

}