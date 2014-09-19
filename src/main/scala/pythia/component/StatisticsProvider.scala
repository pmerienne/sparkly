package pythia.component

import org.apache.spark.streaming.Milliseconds
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import pythia.core._

object Operation extends Enumeration {
  type OperationType = Value
  val Mean, Count = Value

  val statistics = Map[OperationType, Statistic] (
    Mean -> MeanState(0.0, 0),
    Count -> CountState(0)
  )
}

class StatisticsProvider extends Component {

  def metadata = ComponentMetadata (
    name = "Statistic provider", category = "Analytics",
    inputs = Map (
      "Input" -> InputStreamMetadata(namedFeatures = List("Compute on", "Group by"))
    ),
    outputs = Map (
      "Output" -> OutputStreamMetadata(from = Some("Input"), namedFeatures =  List("Result"))
    ),
    properties = Map (
      "Operation" -> PropertyMetadata("STRING"),
      "Window length (in ms)" -> PropertyMetadata("INTEGER"),
      "Slide interval (in ms)" -> PropertyMetadata("INTEGER")
    )
  )

  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val windowLengthMs = context.properties( "Window length (in ms)")
    val slideInterval  = context.properties( "Slide interval (in ms)")

    val out = if(windowLengthMs.isDefined && slideInterval.isDefined) {
      computeStatistics(context.dstream("Input").window(Milliseconds(windowLengthMs.as[Long]), Milliseconds(slideInterval.as[Long])), context)
    } else {
      computeStatistics(context.dstream("Input"), context)
    }

    Map("Output" -> out)
  }

  def computeStatistics(dstream:DStream[Instance], context:Context):DStream[Instance] = {
    val initialStatistic = Operation.statistics(Operation.withName(context.properties("Operation").as[String]))

    val groupedInstances = dstream.map(instance => (stateKey(instance, context), instance))

      val states = groupedInstances
        .map{case (key, instance) => (key, instance.inputFeature("Compute on").as[Double])}
        .updateStateByKey[Statistic]((newValues: Seq[Double], previousState: Option[Statistic]) => {
          Some(previousState.getOrElse(initialStatistic).update(newValues))
        })


    groupedInstances
      .leftOuterJoin(states)
      .map{
        case (key, (instance, Some(stat))) => instance.outputFeature("Result", stat.value)
        case (key, (instance, None)) => instance.outputFeature("Result", null)
      }
  }

  def stateKey(instance:Instance, context:Context):String = if (context.inputFeatureMapped("Input", "Group by")) {
    instance.inputFeature("Group by").as[String]
  } else {
    context.properties("Operation").as[String]
  }
}

case class MeanState(sum:Double, count:Long) extends Statistic {
  def value():Double = sum / count.toDouble
  def update(values: Seq[Double]): MeanState = MeanState(sum + values.reduce(_+_), count + values.size)
}

case class CountState(count:Long) extends Statistic {
  def value():Double = count.toDouble
  def update(values: Seq[Double]): CountState = CountState(count + values.size)
}

trait Statistic {
  def value():Double
  def update(values: Seq[Double]): Statistic
}