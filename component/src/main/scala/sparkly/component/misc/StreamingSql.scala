package sparkly.component.misc

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.dstream.DStream
import sparkly.core._
import org.apache.spark.rdd.RDD
import scala.collection.mutable

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
    val query = context.property("Query").as[String]
    val outputFields = context.outputFeatureNames("Output", "Fields")

    val schemas = (1 to STREAM_COUNT).map(i => StructType(context.inputFeatureNames("Stream" + i, "Fields").map(fieldName => StructField(fieldName, StringType, true))))
    val sqlContext = new SQLContext(context.sc)

    // Create tables
    val streams = (1 to STREAM_COUNT).map{ i =>
      context.dstream("Stream" + i).transform { rdd =>
        val schema = schemas(i - 1)
        val rowRDD = rdd.map(instance => Row.fromSeq(instance.inputFeatures("Fields").asStringList))
        sqlContext.createDataFrame(rowRDD, schema).registerTempTable("stream" + i)

        rdd.sparkContext.emptyRDD[Any]
      }
    }.reduce(_ union _)

    // Query tables
    val output = streams.transform { rdd =>
      val results = sqlContext.sql(query).map(row => Instance((outputFields zip row.toSeq).toMap)).cache()
      results.take(1) // We need such kind of operations however there's not output to transform!

      // Clear table
      (1 to STREAM_COUNT).foreach(i => sqlContext.dropTempTable("stream" + i))
      results
    }

    Map("Output" -> output)
  }
}