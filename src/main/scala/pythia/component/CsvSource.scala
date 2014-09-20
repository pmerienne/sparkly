package pythia.component

import org.apache.spark.streaming.dstream.DStream
import pythia.core._

class CsvSource extends Component {

  override def metadata =  ComponentMetadata (
    name = "Csv source", description = "Read and parse a given csv file", category = "Source",
    outputs = Map (
      "Instances" -> OutputStreamMetadata(listedFeatures = List("Features"))
    ),
    properties = Map (
      "File" -> PropertyMetadata("STRING"),
      "Separator" -> PropertyMetadata("STRING", defaultValue = Some(";"), acceptedValues = List(";", ","))
    )
  )

  override def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val file = context.properties("File").as[String]
    val separator = context.properties("Separator").as[String]

    val featureNames = context.outputMappers("Instances").featuresNames("Features")

    val stream = context.ssc.textFileStream(file)
      .map(line => line.split(separator).toList)
      .map(values => {
        val entries = values.zipWithIndex
          .filter { case (value, index) => index < featureNames.size}
          .map { case (value, index) => featureNames(index) -> value}
          .toMap

        Instance(entries)
      })

    Map("Instances" -> stream)
  }
}
