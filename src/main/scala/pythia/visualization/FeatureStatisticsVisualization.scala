package pythia.visualization

import pythia.core._
import pythia.core.PropertyType._
import pythia.core.VisualizationMetadata
import pythia.core.PropertyMetadata
import org.apache.spark.streaming.Milliseconds
import org.apache.mahout.math.stats.OnlineSummarizer

class FeatureStatisticsVisualization extends Visualization {

  def metadata = VisualizationMetadata (
    name = "Feature statistics",
    properties = Map("Window length (in ms)" -> PropertyMetadata(LONG)),
    features = List("Number feature")
  )

  override def init(context: VisualizationContext): Unit = {
    val dstream = context.features("Number feature")
    val windowDuration = context.properties("Window length (in ms)").as[Long]
    val dataCollector = context.dataCollector

    dstream
      .window(Milliseconds(windowDuration))
      .foreachRDD((rdd, time) => {
        val features = rdd.cache()
        val totalCount = features.count.toDouble
        val definedFeatures = features.filter(_.isDefined).map(_.as[Double]).cache()

        val definedCount = definedFeatures.count
        val missing = totalCount - definedCount.toDouble

        if(definedCount > 0) {
          val advancedStats = new OnlineSummarizer() with Serializable
          definedFeatures.collect().foreach(advancedStats.add)

          val data = Map(
            "count" -> totalCount,
            "missing" -> missing,
            "mean" -> advancedStats.getMean,
            "std" -> advancedStats.getSD,
            "min" -> advancedStats.getMin,
            "max" -> advancedStats.getMax,
            "quantile 0.25" -> advancedStats.quantile(0.25),
            "quantile 0.50" -> advancedStats.quantile(0.50),
            "quantile 0.75" -> advancedStats.quantile(0.75),
            "quantile 0.90" -> advancedStats.quantile(0.90),
            "quantile 0.99" -> advancedStats.quantile(0.99)
          )

          dataCollector.push(time.milliseconds, data)
        }
      })
  }
}

