package pythia.visualization

import pythia.core._
import pythia.core.PropertyType._
import pythia.core.VisualizationMetadata
import pythia.core.PropertyMetadata
import org.apache.spark.streaming.Milliseconds
import org.apache.spark.streaming.StreamingContext._

class CategoricalVisualization extends Visualization {

  implicit def orderingByName: Ordering[(String, Double)] = Ordering.by(count => count._2)

  def metadata = VisualizationMetadata (
    name = "Categorical distribution",
    properties = Map("Window length (in ms)" -> PropertyMetadata(LONG), "Max category (0 for unlimited)" -> PropertyMetadata(INTEGER, Some(0))),
    features = List("Categorical feature (String, Boolean)")
  )

  override def init(context: VisualizationContext): Unit = {
    val dstream = context.features("Categorical feature (String, Boolean)")
    val windowDuration = context.properties("Window length (in ms)").as[Long]
    val max = context.properties("Max category (0 for unlimited)").as[Int]
    val dataCollector = context.dataCollector

    dstream
      .window(Milliseconds(windowDuration))
      .map{f => (f.or("$MISSING_FEATURE$"), 1.0)}
      .reduceByKey(_ + _)
      .foreachRDD((rdd, time) => {
        val total = if(rdd.count() > 0) rdd.map(_._2).reduce(_ + _) else 0.0
        val counts = if(max > 0) {
          rdd.top(max).toMap
        } else {
          rdd.collect().toMap
        }

        val data = counts ++ Map("$TOTAL$" -> total)
        dataCollector.push(time.milliseconds, data)
      })
  }
}
