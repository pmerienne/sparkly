package pythia.core


import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.joda.time.DateTime

abstract class Component extends Serializable {
  def metadata: ComponentMetadata

  protected def initStreams(context: Context): Map[String, DStream[Instance]]

  def init(ssc: StreamingContext, configuration: ComponentConfiguration, inputs: Map[String, DStream[Instance]]) = {

    val inputMappers =  metadata.inputs.map{case (name, meta) =>
      val conf = configuration.inputs.get(name)
      val namedFeatures = conf.flatMap(f => Some(f.mappedFeatures)).getOrElse(Map())
      val listedFeatures = conf.flatMap(f => Some(f.selectedFeatures)).getOrElse(Map())
      (name, Mapper(namedFeatures, listedFeatures))
    }

    val outputMappers =  metadata.outputs.map{case (name, meta) =>
      val conf = configuration.outputs.getOrElse(name, StreamConfiguration())
      (name, Mapper(conf.mappedFeatures, conf.selectedFeatures))
    }

    val properties: Map[String, Property] = metadata.properties.map{prop =>
      val value = configuration.properties.get(prop._1)
      (prop._1, Property(prop._2, value))
    }.toMap[String, Property]

    initStreams(Context(inputs, inputMappers, outputMappers, properties, ssc))
  }

}

case class Context (
  inputs: Map[String, DStream[Instance]],
  inputMappers: Map[String, Mapper],
  outputMappers: Map[String, Mapper],
  properties: Map[String, Property],
  ssc: StreamingContext) {

  def property(name: String) = properties(name)

  def dstream(input: String): DStream[Instance] = {
    val dstream = inputs(input)
    val inputMapper = inputMappers(input)
    dstream.map(_.copy(inputMapper = Some(inputMapper)))
  }

  def dstream(input: String, output: String): DStream[Instance] = {
    val dstream = inputs(input)
    val inputMapper = inputMappers(input)
    val outputMapper = outputMappers(output)
    dstream.map(_.copy(inputMapper = Some(inputMapper), outputMapper = Some(outputMapper)))
  }

  def inputSize(input: String, features: String): Int = inputMappers(input).size(features)
  def inputFeatureMapped(input: String, feature: String): Boolean = inputMappers(input).isFeatureMapped(feature)
  def inputFeaturesMapped(input: String, features: String): Boolean = inputMappers(input).areFeaturesMapped(features)

  def outputSize(output: String, features: String): Int = outputMappers(output).size(features)
  def outputFeatureMapped(output: String, feature: String): Boolean = outputMappers(output).isFeatureMapped(feature)
  def outputFeaturesMapped(output: String, features: String): Boolean = outputMappers(output).areFeaturesMapped(features)
}

case class Mapper(namedFeatures: Map[String, String] = Map(), listedFeatures: Map[String, List[String]] = Map()) {
  def featureName(name: String): String = namedFeatures.getOrElse(name, null)
  def featuresNames(name: String): List[String] = listedFeatures.getOrElse(name, List())
  def size(name:String): Int = featuresNames(name).size

  def isFeatureMapped(name: String): Boolean = namedFeatures.contains(name)
  def areFeaturesMapped(name: String): Boolean = listedFeatures.contains(name)
}

case class Property (
  propertyType: String,
  defaultValue: Option[_] = None,
  selectedValue: Option[String] = None){

  def isDefined(): Boolean = selectedValue.isDefined || defaultValue.isDefined
  def isEmpty(): Boolean = selectedValue.isEmpty

  def get(): Any = selectedValue match {
    case None => defaultValue.getOrElse(null)
    case Some(str: String) => propertyType match {
      case "DECIMAL" => str.toDouble
      case "STRING" => str
      case "INTEGER" => str.toInt
      case "DATE" => DateTime.parse(str).toDate
      case "BOOLEAN" => str.toBoolean
    }
  }

  def as[V] = get.asInstanceOf[V]
}

object Property {
  def apply(metadata: PropertyMetadata, value: Option[String]) = new Property(metadata.propertyType, metadata.defaultValue, value)
}