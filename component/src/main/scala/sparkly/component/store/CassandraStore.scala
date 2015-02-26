package sparkly.component.store

import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import org.apache.spark.streaming.dstream.DStream
import sparkly.core._
import com.datastax.driver.core._
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.writer.{CassandraRowWriter, DefaultRowWriter, WriteConf}
import scala.Some
import com.datastax.spark.connector.cql.CassandraConnectorConf._
import java.net.InetAddress

class CassandraStore extends Component {

  override def metadata = ComponentMetadata (
    name = "Cassandra store", description = "Write stream into cassandra.",
    category = "Stores",
    inputs = Map (
      "In" -> InputStreamMetadata(listedFeatures = Map("Features" -> FeatureType.ANY))
    ),
    properties = Map (
      "Keyspace" -> PropertyMetadata(PropertyType.STRING),
      "Table" -> PropertyMetadata(PropertyType.STRING),
      "Hosts" -> PropertyMetadata(PropertyType.STRING, description = "Comma separated list of hosts"),
      "Native port" -> PropertyMetadata(PropertyType.INTEGER, defaultValue = Some(DefaultNativePort), description = "Cassandra native port"),
      "Username" -> PropertyMetadata(PropertyType.STRING, mandatory = false),
      "Password" -> PropertyMetadata(PropertyType.STRING, mandatory = false),
      "Consistency" -> PropertyMetadata(PropertyType.STRING, defaultValue = Some(WriteConf.DefaultConsistencyLevel.name), acceptedValues = ConsistencyLevel.values().map(_.name).toList),
      "Query retry count"-> PropertyMetadata(PropertyType.INTEGER, defaultValue = Some(DefaultQueryRetryCount), description = "Number of times to retry a timed-out query"),
      "Reconnection delay millis (min)"-> PropertyMetadata(PropertyType.INTEGER, defaultValue = Some(DefaultMinReconnectionDelayMillis), description = "Minimum period of time to wait before reconnecting to a dead node"),
      "Reconnection delay millis (max)"-> PropertyMetadata(PropertyType.INTEGER, defaultValue = Some(DefaultMaxReconnectionDelayMillis), description = "Maximum period of time to wait before reconnecting to a dead node"),
      "Connection timeout" -> PropertyMetadata(PropertyType.INTEGER, defaultValue = Some(DefaultConnectTimeoutMillis), description = "Maximum period of time to attempt connecting to a node"),
      "Read timeout" -> PropertyMetadata(PropertyType.INTEGER, defaultValue = Some(DefaultReadTimeoutMillis), description = "Maximum period of time to wait for a read to return")
    )
  )

  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val featureNames = context.inputFeatureNames("In", "Features")
    val featureColumnNames = featureNames.map(f => ColumnName(f))

    val keyspace = context.property("Keyspace").as[String]
    val table = context.property("Table").as[String]

    val username = if(context.property("Username").or("").isEmpty) None else Some(context.property("Username").as[String])
    val password = Some(context.property("Password").or(""))
    val authConf = (username, password) match {
      case (Some(username), Some(password)) => PasswordAuthConf(username, password)
      case _ =>  NoAuthConf
    }

    val writeConf = WriteConf(consistencyLevel = ConsistencyLevel.valueOf(context.property("Consistency").as[String]))
    val connector = CassandraConnector(
      hosts = context.property("Hosts").as[String].split(",").map(InetAddress.getByName).toSet,
      nativePort = context.property("Native port").as[Int],
      authConf = authConf,
      minReconnectionDelayMillis = context.property("Reconnection delay millis (min)").as[Int],
      maxReconnectionDelayMillis = context.property("Reconnection delay millis (max)").as[Int],
      queryRetryCount = context.property("Query retry count").as[Int],
      connectTimeoutMillis = context.property("Connection timeout").as[Int],
      readTimeoutMillis = context.property("Read timeout").as[Int]
    )

    context
      .dstream("In")
      .map{instance =>
        val columns =  featureNames
        val values = instance.inputFeatures("Features").asRaw
        CassandraRow.fromMap((columns zip values).toMap)
      }.saveToCassandra(keyspace, table,  SomeColumns(featureColumnNames:_*), writeConf)(connector, CassandraRowWriter.Factory)

    Map()
  }

}
