package sparkly.core

import com.codahale.metrics.Gauge

class Monitoring(val name: String) extends Serializable {

  private val data = collection.mutable.Map[String, Double]()
  private var timestamp: Option[Long] = None

  def createMetric(): Gauge[MonitoringData] = new Gauge[MonitoringData] {
    override def getValue() = MonitoringData(timestamp.getOrElse(System.currentTimeMillis()), data.toMap)
  }

  def set(timestamp: Long): Unit = {
    this.timestamp = Some(timestamp)
  }

  def add(name: String, value: Double): Unit = data.update(name, value)
  def add(elem: (String, Double)): Unit = data.update(elem._1, elem._2)
  
  def set(elems: (String, Double)*): Unit = set(elems.toMap)
  def set(timestamp: Long, elems: (String, Double)*): Unit = set(timestamp, elems.toMap)
  def set(elems: Map[String, Double]): Unit = set(System.currentTimeMillis, elems)
  def set(timestamp: Long, elems: Map[String, Double]): Unit = {
    set(timestamp)
    clear()
    elems.foreach(add)
  }
  
  def remove(name: String): Unit = data.remove(name)
  def clear(): Unit = data.clear()

}

case class MonitoringData(timestamp: Long, data: Map[String, Double])

object Monitoring {
  def cleanName(name: String): String = name.replaceAll("[^A-Za-z0-9]", "-").toLowerCase
}