package sparkly.component.store

import com.datastax.spark.connector.util.Logging
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s._
import org.apache.spark.TaskContext
import org.apache.spark.streaming.dstream._
import org.elasticsearch.common.settings.ImmutableSettings
import sparkly.core._

import scala.collection.mutable
import scala.concurrent._
import scala.concurrent.duration._

class ESStore extends Component {

  override def metadata = ComponentMetadata (
    name = "Elasticsearch store", description = "Write stream into Elasticsearch.",
    category = "Stores",
    inputs = Map (
      "In" -> InputStreamMetadata(listedFeatures = Map("Features" -> FeatureType.ANY))
    ),
    properties = Map (
      "Hosts" -> PropertyMetadata(PropertyType.STRING, description = "Comma separated list of remote with ports (i.e host:port,host:port,...)"),
      "Cluster name" -> PropertyMetadata(PropertyType.STRING, mandatory = false),
      "Index" -> PropertyMetadata(PropertyType.STRING)
    )
  )

  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val featureNames = context.inputFeatureNames("In", "Features")
    val hosts = context.property("Hosts").as[String]
    val clusterName = Some(context.property("Cluster name").as[String])
    val indexName = context.property("Index").as[String]
    val writer = new ESWriter(hosts, clusterName, indexName)

    context
      .dstream("In")
      .map(instance => (featureNames zip instance.inputFeatures("Features").asRaw).toMap)
      .foreachRDD(rdd => rdd.sparkContext.runJob(rdd, writer.write _))

    Map()
  }

}


class ESWriter(hosts: String, clusterName: Option[String], indexName: String, timeout: Duration = 10 seconds) extends Serializable with Logging {

  def write(taskContext: TaskContext, data: Iterator[Map[String, Any]]) {
    val client = createClient()
    taskContext.addTaskCompletionListener((context: TaskContext) => client.close())

    import scala.concurrent.ExecutionContext.Implicits.global

    val futures = mutable.MutableList[Future[Any]]()
    while(data.hasNext) {
      val future = client.execute{index into indexName fields data.next}
      futures += future
    }

    Await.ready(Future.sequence(futures), timeout)
  }

  def createClient(): ElasticClient = {
    val uri = ElasticsearchClientUri(s"elasticsearch://$hosts")

    val settings = ImmutableSettings.settingsBuilder()
    if(clusterName.isDefined) {
      settings.put("cluster.name", clusterName.get)
    }

    ElasticClient.remote(settings.build(), uri)
  }
}