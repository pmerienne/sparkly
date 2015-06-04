package sparkly.component.preprocess

import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import sparkly.core.PropertyType
import sparkly.core.PropertyType._

import sparkly.core._


class Inputer extends Component {

  def metadata = ComponentMetadata(
    name = "Inputer", category = "Pre-processor",
    description = "Handle missing number values.",
    inputs = Map (
      "Input" -> InputStreamMetadata(listedFeatures = Map("Features" -> FeatureType.NUMBER))
    ),
    outputs = Map (
      "Output" -> OutputStreamMetadata(from = Some("Input"))
    ),
    properties = Map(
      "Parallelism" -> PropertyMetadata(INTEGER, defaultValue = Some(-1), description = "Level of parallelism to use. -1 to use default level."),
      "Missing" -> PropertyMetadata(PropertyType.STRING, mandatory = false, defaultValue = Some(""), description = "Placeholder for the missing values. Empty features and all occurrences of this value will be handled."),
      "Strategy" -> PropertyMetadata(PropertyType.STRING, defaultValue = Some("Mean"), acceptedValues = List("Mean", "Default", "Filter")),
      "Default value" -> PropertyMetadata(PropertyType.DECIMAL, mandatory = false, description = "Default value use to impute missing value when 'Default' strategy is choose")
    )
  )

  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val stream = context.dstream("Input", "Output")
    val partitions = context.property("Parallelism").as[Int] match {
      case positive if positive > 0 => positive
      case _ => context.sc.defaultParallelism
    }
    val detector = MissingValueDetector(context.property("Missing").as[String] match {
      case "" => None
      case "NaN" => Some(Double.NaN)
      case _ @str => Some(str.toDouble)
    })

    val out = context.property("Strategy").as[String] match {
      case "Mean" => initMeanStrategyStream(stream, detector, partitions)
      case "Default" => initDefaultStrategyStream(stream, detector, context.property("Default value").as[Double])
      case "Filter" => initFilterStrategyStream(stream, detector)
    }

    Map("Output" -> out)
  }

  private def initMeanStrategyStream(stream: DStream[Instance], detector: MissingValueDetector, partitions: Int) : DStream[Instance] = {
    val features: DStream[(Int, (Feature[_], Instance))] = stream.flatMap{ i => i.inputFeatures("Features").values.zipWithIndex.map{case (feature, index) => (index, (feature, i))} }

    val means = features.updateStateByKey((values: Seq[(Feature[_], Instance)], previous: Option[RunningMean]) => {
      val state = previous.getOrElse(RunningMean())
      Some(state + values.map(_._1).filter(f => !detector.isMissing(f)).map(_.as[Double]))
    }, partitions)

    features
      .join(means, partitions)
      .map{case (index, ((feature, instance), mean)) =>
        val imputed = if(detector.isMissing(feature)) mean.mean else feature.as[Double]
        (instance.uuid, (instance, imputed, index))
      }
      .groupByKey(partitions)
      .map{case (uuid, data) =>
        val instance = data.head._1
        val features = data.toList.sortBy(_._3).map(_._2)
        instance.inputFeatures("Features", features)
      }
  }

  private def initDefaultStrategyStream(stream: DStream[Instance], detector: MissingValueDetector, default: Double) : DStream[Instance] = {
    stream.map{ instance =>
      val features = instance.inputFeatures("Features").values.map(f => if(detector.isMissing(f)) default else f.as[Double])
      instance.inputFeatures("Features", features)
    }
  }

  private def initFilterStrategyStream(stream: DStream[Instance], detector: MissingValueDetector) : DStream[Instance] = {
    stream.filter(i => !i.inputFeatures("Features").values.exists(f => detector.isMissing(f)))
  }

}

case class RunningMean(n: Long = 0L, sum: Double = 0.0) {
  def +(values: Seq[Double]): RunningMean = this.copy(n = n + values.size, sum = sum + values.sum)
  def mean: Double = sum / n
}

case class MissingValueDetector(missing: Option[Double]) {
  def isMissing(feature: Feature[_]): Boolean = feature.isEmpty || (missing.isDefined && feature.as[Double] == missing.get)
}