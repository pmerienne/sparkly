package sparkly.component.store

import com.datastax.spark.connector.streaming._
import org.joda.time.DateTime
import sparkly.core._
import sparkly.testing._

class CassandraStoreSpec extends ComponentSpec with SparklyEmbeddedCassandra {

  "Cassandra store " should "save features to cassandra" in {
    // Given
    val configuration = ComponentConfiguration (
      name = "Cassandra store",
      clazz = classOf[CassandraStore].getName,
      inputs = Map (
        "In" -> StreamConfiguration(selectedFeatures = Map("Primary keys" -> List("stationid", "timestamp"), "Features" -> List("temperature")))
      ),
      properties = Map (
        "Keyspace" -> "cassandra_store_spec",
        "Table" -> "temperature"
      )
    )

    conn.withSessionDo{ session =>
      session.execute("DROP KEYSPACE IF EXISTS cassandra_store_spec")
      session.execute("CREATE KEYSPACE cassandra_store_spec WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }")
      session.execute("CREATE TABLE cassandra_store_spec.temperature (stationid TEXT, timestamp TIMESTAMP, temperature INT, PRIMARY KEY (stationid, timestamp))")
    }

    // When
    val component = deployComponent(configuration)
    component.inputs("In").push(
      Instance("stationid" -> "0", "timestamp" -> "2015-02-19 21:47:18", "temperature" -> 8),
      Instance("stationid" -> "0", "timestamp" -> "2015-02-19 21:47:28", "temperature" -> 9),
      Instance("stationid" -> "1", "timestamp" -> "2015-02-19 21:47:18", "temperature" -> 19)
    )

    // Then
    eventually {
      val actualTemperatures = ssc.get
        .cassandraTable("cassandra_store_spec", "temperature")
        .map(row => (row.getString(0), row.getDateTime(1), row.getInt(2))).collect()

      actualTemperatures should contain only (
        ("0", DateTime.parse("2015-02-19T21:47:18"), 8),
        ("0", DateTime.parse("2015-02-19T21:47:28"), 9),
        ("1", DateTime.parse("2015-02-19T21:47:18"), 19)
      )
    }
  }

}
