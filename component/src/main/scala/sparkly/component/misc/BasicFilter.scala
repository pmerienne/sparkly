package sparkly.component.misc

import sparkly.core._
import org.apache.spark.streaming.dstream.DStream
import sparkly.core.Context
import sparkly.core.ComponentMetadata

class BasicFilter extends Component {

  override def metadata: ComponentMetadata = ComponentMetadata(
    name = "Basic Filter", description = "Keeps only instances that satisfy a predicate.", category = "Miscellaneous",
    properties = Map(
      "Operator" -> PropertyMetadata(PropertyType.STRING, defaultValue = Some("IS NULL"), acceptedValues = List("=", ">", ">=", "<", "<=", "!=", "IS NULL", "IS NOT NULL")),
      "Operand" -> PropertyMetadata(PropertyType.STRING)
    ),
    inputs = Map("In" -> InputStreamMetadata(namedFeatures = Map("Feature" -> FeatureType.STRING))),
    outputs = Map("Out" -> OutputStreamMetadata(from = Some("In")))
  )

  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val predicate = createPredicate(context.properties)
    Map("Out" -> context.dstream("In").filter(instance => predicate(instance.inputFeature("Feature"))))
  }

  def createPredicate(properties: Map[String, Property]): (Feature[Any]) => Boolean = properties("Operator").as[String] match {
    case "IS NULL" => (f: Feature[Any]) => f.isEmpty
    case "IS NOT NULL" => (f: Feature[Any]) => f.isDefined
    case "=" => (f: Feature[Any]) => f.as[String] == properties("Operand").as[String]
    case ">" => (f: Feature[Any]) => f.as[String] > properties("Operand").as[String]
    case ">=" => (f: Feature[Any]) => f.as[String] >= properties("Operand").as[String]
    case "<" => (f: Feature[Any]) => f.as[String] < properties("Operand").as[String]
    case "<=" => (f: Feature[Any]) => f.as[String] <= properties("Operand").as[String]
    case "!=" => (f: Feature[Any]) => f.as[String] != properties("Operand").as[String]
  }
}