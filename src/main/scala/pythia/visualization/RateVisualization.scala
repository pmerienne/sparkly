package pythia.visualization

import pythia.core._
import pythia.core.VisualizationContext
import pythia.core.PropertyType._
import org.apache.spark.streaming.Milliseconds
import pythia.core.VisualizationMetadata
import pythia.core.PropertyMetadata
import pythia.core.VisualizationContext

class RateVisualization extends Visualization {

  def metadata = VisualizationMetadata (
    name = "Rate",
    properties = Map("Window length (in ms)" -> PropertyMetadata(LONG)),
    streams = List("Stream")
  )

  override def init(context: VisualizationContext): Unit = {
    val stream = context.streams("Stream")
    val windowDuration = context.properties("Window length (in ms)").as[Long]
    val dataCollector = context.dataCollector

    stream
      .window(Milliseconds(windowDuration))
      .foreachRDD((rdd, time) => {
        val rate = 1000 * rdd.count.toDouble / windowDuration.toDouble
        dataCollector.push(time.milliseconds, Map("rate" -> rate))
      })
  }
}
