package sparkly.core

import org.scalatest._

class MonitoringSpec extends FlatSpec with Matchers {

  "Monitoring" should "support add data" in {
    // Given
    val monitoring = new Monitoring("test")
    val gauge = monitoring.createMetric()

    // When
    monitoring.add("value1", 1.0)
    monitoring.add("value2" -> 2.0)
    monitoring.add("value3" -> 3.0)
    monitoring.add("value3" -> 3.3)
    monitoring.remove("value2")

    // Then
    gauge.getValue.data should contain only (
      "value1" -> 1.0, "value3" -> 3.3
    )
  }

  "Monitoring" should "support set data" in {
    // Given
    val monitoring = new Monitoring("test")
    val gauge = monitoring.createMetric()

    // When
    monitoring.set("value1" -> 1.0, "value2" -> 2.0)
    monitoring.set("value1" -> 1.1, "value2" -> 2.2)

    // Then
    gauge.getValue.data should contain only (
      "value1" -> 1.1, "value2" -> 2.2
    )
  }
}
