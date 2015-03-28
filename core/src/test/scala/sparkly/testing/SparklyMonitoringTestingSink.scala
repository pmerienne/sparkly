package org.apache.spark.metrics.sink

import com.codahale.metrics._
import java.util.concurrent.TimeUnit
import java.util.{Properties, SortedMap}
import scala.collection.JavaConversions._
import sparkly.core.MonitoringData

class SparklyMonitoringTestingSink(val property: Properties, val registry: MetricRegistry, securityMgr: org.apache.spark.SecurityManager) extends Sink {

  val reporter = new SparklyMonitoringTestingReporter(registry)

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

class SparklyMonitoringTestingReporter (
    registry: MetricRegistry,
    name: String = "monitoring-testing-reporter",
    filter: MetricFilter = MetricFilter.ALL,
    rateUnit: TimeUnit = TimeUnit.SECONDS,
    durationUnit: TimeUnit = TimeUnit.MILLISECONDS)
  extends ScheduledReporter(
    registry: MetricRegistry,
    name: String,
    filter: MetricFilter,
    rateUnit: TimeUnit,
    durationUnit: TimeUnit) {

  override def report(
     gauges: SortedMap[String, Gauge[_]],
     counters: SortedMap[String, Counter],
     histograms: SortedMap[String, Histogram],
     meters: SortedMap[String, Meter],
     timers: SortedMap[String, Timer]): Unit = {

    // TODO : We may have multiple gauges! 1 per worker ??!?

    gauges
      .filter(_._1 contains "monitoring.")
      .foreach{case (name, gauge) =>
      val Array(sourceId, monitoringId) = name.split("-monitoring.")
      val data = gauge.getValue.asInstanceOf[MonitoringData]
      MonitoringTestingData.add(monitoringId, data)
    }

  }

}

import collection.mutable.{HashMap, MultiMap, Set}

object MonitoringTestingData {

  private val allData = new HashMap[String, Set[MonitoringData]] with MultiMap[String, MonitoringData]

  def all(monitoringId: String): List[MonitoringData] = allData(monitoringId).toList.sortBy(_.timestamp)
  def latest(monitoringId: String) : MonitoringData = all(monitoringId).last

  def add(monitoringId: String, data: MonitoringData): Unit = {
    allData.addBinding(monitoringId, data)
  }

  def clear(): Unit = allData.clear()

}