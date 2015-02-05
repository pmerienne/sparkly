package pythia.visualization

import pythia.core._
import pythia.core.PropertyType._
import pythia.core.VisualizationMetadata
import pythia.core.PropertyMetadata
import org.apache.spark.streaming.Milliseconds
import scala.util.Try
import pythia.utils.FeatureStatistics

class FeatureStatisticsVisualization extends Visualization {

  def metadata = VisualizationMetadata (
    name = "Feature statistics",
    properties = Map(
      "Window length (in ms)" -> PropertyMetadata(LONG),
      "Parallelism" -> PropertyMetadata(INTEGER, defaultValue = Some(-1), description = "Level of parallelism to use. -1 to use default level.")
    ),
    features = List("Number feature")
  )

  override def init(context: VisualizationContext): Unit = {
    val dstream = context.features("Number feature")
    val windowDuration = context.properties("Window length (in ms)").as[Long]
    val dataCollector = context.dataCollector
    val partitions = context.property("Parallelism").or(context.sc.defaultParallelism, on = (parallelism: Int) => parallelism < 1)

    dstream
      .repartition(partitions)
      .mapPartitions(features => Iterator(FeatureStatistics(features)))
      .reduceByWindow((stat1, stat2) => stat1.merge(stat2), Milliseconds(windowDuration), dstream.slideDuration)
      .foreachRDD((rdd, time) => {
        val stats = Try(rdd.take(1)(0)).getOrElse(FeatureStatistics.zero())
        val data = Map(
          "count" -> stats.count.toDouble,
          "missing" -> stats.missing.toDouble,
          "mean" -> stats.mean,
          "std" -> stats.stdev,
          "min" -> stats.min,
          "max" -> stats.max,
          "quantile 0.25" -> stats.quantile(0.25),
          "quantile 0.50" -> stats.quantile(0.50),
          "quantile 0.75" -> stats.quantile(0.75),
          "quantile 0.90" -> stats.quantile(0.90),
          "quantile 0.99" -> stats.quantile(0.99)
        )
        dataCollector.push(time.milliseconds, data)
      })
  }
}

