package org.apache.spark.metrics.sink

import java.util.Properties
import com.codahale.metrics._
import java.util.concurrent.TimeUnit
import java.util
import pythia.core.VisualizationDataCollector
import pythia.config.PythiaConfig

class JvmVisualizationSink(val property: Properties, val registry: MetricRegistry, securityMgr: org.apache.spark.SecurityManager) extends Sink {

  val clusterId = "local"
  val reporter = new JvmVisualizationReporter(clusterId, "pipeline-memory", registry)

  override def start(): Unit = {
    reporter.start(10, TimeUnit.SECONDS)
  }

  override def stop(): Unit = {
    reporter.stop()
  }

  override def report(): Unit = {
    reporter.report()
  }
}

class JvmVisualizationReporter (clusterId: String, visualizationId: String, registry: MetricRegistry,
    name: String = "visualization-reporter",
    filter: MetricFilter = MetricFilter.ALL,
    rateUnit: TimeUnit = TimeUnit.SECONDS,
    durationUnit: TimeUnit = TimeUnit.MILLISECONDS)
  extends ScheduledReporter(
    registry: MetricRegistry,
    name: String, filter: MetricFilter,
    rateUnit: TimeUnit,
    durationUnit: TimeUnit) {

  val dataCollector = new VisualizationDataCollector(PythiaConfig.HOSTNAME, PythiaConfig.WEB_PORT, clusterId, visualizationId)

  override def report(
    gauges: util.SortedMap[String, Gauge[_]],
    counters: util.SortedMap[String, Counter],
    histograms: util.SortedMap[String, Histogram],
    meters: util.SortedMap[String, Meter],
    timers: util.SortedMap[String, Timer]): Unit = {

    val memoryUsed = Option(gauges.get("jvm.total.used")) match {
      case Some(gauge) => gauge.getValue.toString.toDouble
      case None => 0.0
    }

    val memoryCommitted = Option(gauges.get("jvm.total.committed")) match {
      case Some(gauge) => gauge.getValue.toString.toDouble
      case None => 0.0
    }

    val maxMemory = Option(gauges.get("jvm.total.max")) match {
      case Some(gauge) => gauge.getValue.toString.toDouble
      case None => 0.0
    }

    val data = Map ("memory.used" -> memoryUsed, "memory.max" -> maxMemory, "memory.committed" -> memoryCommitted)
    dataCollector.push(System.currentTimeMillis, data)
  }
}
