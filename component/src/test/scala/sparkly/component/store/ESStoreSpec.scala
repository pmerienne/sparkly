package sparkly.component.store

import com.sksamuel.elastic4s.ElasticDsl._
import sparkly.core._
import sparkly.testing._

class ESStoreSpec extends ComponentSpec with EmbeddedElasticsearch {

  "Elasticsearch store " should "write features to ES" in {
    // Given
    elasticsearchServer.createIndex("sensor")

    val configuration = ComponentConfiguration (
      name = "Elasticsearch store",
      clazz = classOf[ESStore].getName,
      inputs = Map (
        "In" -> StreamConfiguration(selectedFeatures = Map("Features" -> List("stationid", "timestamp", "temperature")))
      ),
      properties = Map (
        "Hosts" -> elasticsearchServer.hosts,
        "Cluster name" -> elasticsearchServer.clusterName,
        "Index" -> "sensor"
      )
    )

    // When
    val component = deployComponent(configuration)
    component.inputs("In").push(
      Instance("stationid" -> "0", "timestamp" -> "2015-02-19 21:47:18", "temperature" -> 8),
      Instance("stationid" -> "0", "timestamp" -> "2015-02-19 21:47:28", "temperature" -> 9),
      Instance("stationid" -> "1", "timestamp" -> "2015-02-19 21:47:18", "temperature" -> 19)
    )

    // Then
    eventually {
      val results = elasticsearchClient.execute{search in "sensor"}.await.getHits.getHits.map(hit => hit.getSource)
      import scala.collection.JavaConversions._
      results.map(_.toMap) should contain only (
        Map("stationid" -> "0", "timestamp" -> "2015-02-19 21:47:18", "temperature" -> 8),
        Map("stationid" -> "0", "timestamp" -> "2015-02-19 21:47:28", "temperature" -> 9),
        Map("stationid" -> "1", "timestamp" -> "2015-02-19 21:47:18", "temperature" -> 19)
        )
    }
  }

}
