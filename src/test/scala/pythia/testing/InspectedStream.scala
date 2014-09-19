package pythia.testing

import org.apache.spark.streaming.dstream.DStream
import pythia.core.Instance

import scala.collection.mutable.ListBuffer

case class InspectedStream(instances: ListBuffer[Instance]) {
  def features() = instances.map(_.rawFeatures)
}


object InspectedStream {

  def apply(dstream: DStream[Instance]): InspectedStream = {
    val instances = ListBuffer[Instance]()
    dstream.foreachRDD { rdd => instances ++= rdd.collect()}

    InspectedStream(instances)
  }

  def inspect(dstream: DStream[Instance]) = InspectedStream(dstream).instances.map(_.rawFeatures)
}
