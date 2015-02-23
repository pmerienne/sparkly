package sparkly.testing

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.EmbeddedCassandra

trait SparklyEmbeddedCassandra extends EmbeddedCassandra {

  useCassandraConfig("cassandra-default.yaml.template")

  val conn = CassandraConnector(Set(EmbeddedCassandra.cassandraHost))

  def clearCache(): Unit = CassandraConnector.evictCache()

}