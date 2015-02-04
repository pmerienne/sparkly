package pythia.visualization

import pythia.core._
import pythia.core.VisualizationContext
import pythia.core.PropertyType._
import org.apache.spark.streaming.Milliseconds
import pythia.core.VisualizationMetadata
import pythia.core.PropertyMetadata
import pythia.core.VisualizationContext
import scala.util.Try

class ThroughputVisualization extends Visualization {

  def metadata = VisualizationMetadata (
    name = "Throughput",
    properties = Map("Window length (in ms)" -> PropertyMetadata(LONG)),
    streams = List("Stream")
  )

  override def init(context: VisualizationContext): Unit = {
    val stream = context.streams("Stream")
    val windowDuration = context.properties("Window length (in ms)").as[Long]
    val dataCollector = context.dataCollector
    val partitions = context.ssc.sparkContext.defaultParallelism

    stream
      .repartition(partitions)
      .countByWindow(Milliseconds(windowDuration), stream.slideDuration)
      .foreachRDD((rdd, time) => {
        val count = Try(rdd.take(1)(0)).toOption.getOrElse(0L)
        val throughput = 1000 * count / windowDuration.toDouble
        dataCollector.push(time.milliseconds, Map("throughput" -> throughput))
      })
  }
}
