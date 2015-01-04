package pythia.component.misc

import pythia.core._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import pythia.core.PropertyType.STRING

class Join extends Component {

  override def metadata: ComponentMetadata = ComponentMetadata (
    name = "Join", category = "Miscellaneous",
    description = "Join data from 2 streams, based on common fields between them. This component only apply join within each batch that comes from the input streams. It joins 'Stream 1' and 'Stream 2' together using 'Join features'. Output features should contains (in order) 'Join features', 'Non-Join features' from 'Stream 1' then 'Non-join features' from 'Stream 2'.",
    properties = Map("Type" -> PropertyMetadata(STRING, defaultValue = Some("Inner join"), acceptedValues = List("Inner join", "Left join", "Right join"))),
    inputs = Map (
      "Stream 1" -> InputStreamMetadata(listedFeatures = Map("Join features" -> FeatureType.ANY, "Non-join features" -> FeatureType.ANY)),
      "Stream 2" -> InputStreamMetadata(listedFeatures = Map("Join features" -> FeatureType.ANY, "Non-join features" -> FeatureType.ANY))
    ),
    outputs = Map("Output" -> OutputStreamMetadata(listedFeatures = Map("Join and Non-join features" -> FeatureType.ANY)))
  )

  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val joinType = context.property("Type").as[String]
    val outputMapper = context.outputMappers("Output")
    val leftFeatureSize = context.inputSize("Stream 1", "Non-join features")
    val rightFeatureSize = context.inputSize("Stream 2", "Non-join features")

    val stream1 = context.dstream("Stream 1").map(instance => (instance.inputFeatures("Join features"), instance.inputFeatures("Non-join features")))
    val stream2 = context.dstream("Stream 2").map(instance => (instance.inputFeatures("Join features"), instance.inputFeatures("Non-join features")))

    // TODO : enable partitioning
    val joinedStream = joinType match {
      case "Inner join" => stream1.join(stream2).map(data => (data._1, data._2._1, data._2._2))
      case "Left join" => stream1.leftOuterJoin(stream2).map(data => (data._1, data._2._1, data._2._2.getOrElse(emptyFeatureList(rightFeatureSize))))
      case "Right join" => stream1.rightOuterJoin(stream2).map(data => (data._1, data._2._1.getOrElse(emptyFeatureList(leftFeatureSize)), data._2._2))
    }

    val outputStream = joinedStream.map{ case (joinFeatures: FeatureList, nonJoinFeatures1: FeatureList, nonJoinFeatures2: FeatureList) =>
      val features = joinFeatures.asRaw ++ nonJoinFeatures1.asRaw ++ nonJoinFeatures2.asRaw
      val namedFeatures = outputMapper.featuresNames("Join and Non-join features") zip features
      new Instance(namedFeatures.toMap)
    }

    Map("Output" -> outputStream)
  }

  private def emptyFeatureList(size: Int): FeatureList = FeatureList(List.fill(size)(Feature(None)))
}
