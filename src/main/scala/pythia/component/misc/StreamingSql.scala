package pythia.component.misc

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.catalyst.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.dstream.DStream
import pythia.core._

class StreamingSql extends Component {
  
  val STREAM_COUNT = 4

  override def metadata: ComponentMetadata = ComponentMetadata (
    name = "Streaming Sql", description = "Run SQL queries over a single stream",
    category = "Miscellaneous",
    inputs = (1 to STREAM_COUNT).map(i => "Stream" + i -> InputStreamMetadata(listedFeatures = Map("Fields" -> FeatureType.STRING))).toMap,
    outputs = Map("Output" -> OutputStreamMetadata(listedFeatures = Map("Fields" -> FeatureType.STRING))),
    properties = Map("Query" -> PropertyMetadata(PropertyType.STRING, description = "SQL query to run. (Stream tables are named 'stream1', 'stream2', ..."))
  )

  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val sqlContext = new SQLContext(context.ssc.sparkContext)
    val query = context.property("Query").as[String]
    val outputFields = context.outputFeatureNames("Output", "Fields")

    val schemas = (1 to STREAM_COUNT).map(i => StructType(context.inputFeatureNames("Stream" + i, "Fields").map(fieldName => StructField(fieldName, StringType, true))))
    val inputs = (1 to STREAM_COUNT).map(i => context.dstream("Stream" + i).map(("stream" + i, _))).reduce(_ union _)

    val output = inputs
      .transform { rdd =>
        (1 to STREAM_COUNT).foreach{i =>
          sqlContext.applySchema(rdd.filter(_._1 == "stream" + i).map(instance => Row.fromSeq(instance._2.inputFeatures("Fields").as[String])), schemas(i - 1)).registerTempTable("stream" + i)
        }

      val results = sqlContext.sql(query).map(row => Instance((outputFields zip row.toList).toMap))
        results.take(1) // We need such kind of operations however there's not output to transform!
        results
      }

    Map("Output" -> output)
  }
}