package pythia.testing

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import pythia.core.{Component, ComponentMetadata, Context, Instance}

import scala.collection.mutable

class MockStream extends Component {

  var componentId: String = null

  private val queue = new mutable.SynchronizedQueue[RDD[Instance]]()
  private var sc: Option[SparkContext] = None

  override def metadata: ComponentMetadata = ComponentMetadata("Fake input stream")

  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    componentId = context.componentId
    sc = Some(context.ssc.sparkContext)
    Map(MockStream.OUTPUT_NAME ->  context.ssc.queueStream(queue))
  }

  def push(instances: Instance*): Unit = push(instances.toList)
  def push(instances: List[Instance]): Unit = sc match{
    case Some(sparkContext) => queue += sparkContext.makeRDD(instances)
    case None => throw new IllegalStateException("You can't push data before the fake input stream is initiated")
  }
}

object MockStream {

  val OUTPUT_NAME = "output"
}
