package sparkly.component.common

import sparkly.core._
import org.apache.spark.streaming.dstream.DStream
import scala.util.Try

object ThroughputMonitoringFactory {
  val unit = "instance/s"

  def create(name: String, stream: DStream[Instance], context: Context) = {
    val throughputMonitoring = context.createMonitoring(name)

    val windowDurationMs = stream.slideDuration.milliseconds
    stream
      .count()
      .foreachRDD((rdd, time) => {
        val count = Try(rdd.take(1)(0)).getOrElse(0L)
        val throughput = count * 1000.0 / windowDurationMs.toDouble
        throughputMonitoring.set(time.milliseconds, "Throughput" -> throughput)
      })
  }

}
