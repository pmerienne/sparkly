package sparkly.visualization

import sparkly.core._
import sparkly.core.PropertyType._
import org.apache.spark.streaming.Milliseconds
import scala.util.Try

class ThroughputVisualization extends Visualization {

  def metadata = VisualizationMetadata (
    name = "Throughput",
    properties = Map(
      "Window length (in ms)" -> PropertyMetadata(LONG),
      "Parallelism" -> PropertyMetadata(INTEGER, defaultValue = Some(-1), description = "Level of parallelism to use. -1 to use default level.")
    ),
    streams = List("Stream")
  )

  override def init(context: VisualizationContext): Unit = {
    val stream = context.streams("Stream")
    val windowDuration = context.properties("Window length (in ms)").as[Long]
    val dataCollector = context.dataCollector
    val partitions = context.property("Parallelism").or(context.sc.defaultParallelism, on = (parallelism: Int) => parallelism < 1)

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
