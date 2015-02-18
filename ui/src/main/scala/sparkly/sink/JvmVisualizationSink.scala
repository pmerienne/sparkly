package org.apache.spark.metrics.sink

import java.util.Properties
import com.codahale.metrics._
import scala.collection.JavaConversions._
import java.util.concurrent.TimeUnit
import java.util
import sparkly.core.VisualizationDataCollector
import sparkly.config.SparklyConfig._

class JvmVisualizationSink(val property: Properties, val registry: MetricRegistry, securityMgr: org.apache.spark.SecurityManager) extends Sink {

  val clusterId = "local"
  val reporter = new JvmVisualizationReporter(clusterId, "pipeline-memory", registry)

  override def start(): Unit = {
    reporter.start(1, TimeUnit.SECONDS)
  }

  override def stop(): Unit = {
    reporter.stop()
  }

  override def report(): Unit = {
    reporter.report()
  }
}

class JvmVisualizationReporter (
    clusterId: String, visualizationId: String, registry: MetricRegistry,
    name: String = "visualization-reporter",
    filter: MetricFilter = MetricFilter.ALL,
    rateUnit: TimeUnit = TimeUnit.SECONDS,
    durationUnit: TimeUnit = TimeUnit.MILLISECONDS)
  extends ScheduledReporter(
    registry: MetricRegistry,
    name: String, filter: MetricFilter,
    rateUnit: TimeUnit,
    durationUnit: TimeUnit) {

  val dataCollector = new VisualizationDataCollector(HOSTNAME, WEB_PORT, clusterId, visualizationId)

  override def report(
    gauges: util.SortedMap[String, Gauge[_]],
    counters: util.SortedMap[String, Counter],
    histograms: util.SortedMap[String, Histogram],
    meters: util.SortedMap[String, Meter],
    timers: util.SortedMap[String, Timer]): Unit = {

    // TODO : We may have multiple gauges! 1 per worker ??!?

    val memoryUsed = gauges.find(_._1 endsWith "jvm.total.used").map(_._2) match {
      case Some(gauge) => gauge.getValue.toString.toDouble
      case None => 0.0
    }

    val memoryCommitted = gauges.find(_._1 endsWith "jvm.total.committed").map(_._2) match {
      case Some(gauge) => gauge.getValue.toString.toDouble
      case None => 0.0
    }

    val maxMemory = gauges.find(_._1 endsWith "jvm.total.max").map(_._2) match {
      case Some(gauge) => gauge.getValue.toString.toDouble
      case None => 0.0
    }

    val data = Map ("memory.used" -> memoryUsed, "memory.max" -> maxMemory, "memory.committed" -> memoryCommitted)
    dataCollector.push(System.currentTimeMillis, data)
  }
}
