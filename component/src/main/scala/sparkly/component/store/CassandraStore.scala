package sparkly.component.store

import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import org.apache.spark.streaming.dstream.DStream
import sparkly.core._

class CassandraStore extends Component {

  override def metadata = ComponentMetadata (
    name = "Cassandra store", description = "Store features in cassandra.",
    category = "Stores",
    inputs = Map (
      "In" -> InputStreamMetadata(listedFeatures = Map("Primary keys" -> FeatureType.ANY, "Features" -> FeatureType.ANY))
    ),
    properties = Map (
      "Keyspace" -> PropertyMetadata(PropertyType.STRING),
      "Table" -> PropertyMetadata(PropertyType.STRING)
    )
  )

  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val keyspace = context.property("Keyspace").as[String]
    val table = context.property("Table").as[String]

    val primaryKeyNames = context.inputFeatureNames("In", "Primary keys")
    val primaryKeyColumnNames = primaryKeyNames.map(f => ColumnName(f))
    val featureNames = context.inputFeatureNames("In", "Features")
    val featureColumnNames = featureNames.map(f => ColumnName(f))

    context
      .dstream("In")
      .map{instance =>
        val columns =  primaryKeyNames ++ featureNames
        val values = instance.inputFeatures("Primary keys").asRaw ++ instance.inputFeatures("Features").asRaw
        CassandraRow.fromMap((columns zip values).toMap)
      }.saveToCassandra(keyspace, table,  SomeColumns((primaryKeyColumnNames ++ featureColumnNames):_*))

    Map()
  }

}
