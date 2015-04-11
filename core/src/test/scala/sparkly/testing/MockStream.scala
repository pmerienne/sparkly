package sparkly.testing

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import sparkly.core._
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}


import ExecutionContext.Implicits.global

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

  def push(rate: Int, generator: () => Instance): Unit = Future {
    while(true) {
      val instances = (1 to rate).map(i => generator()).toList
      push(instances)
      Thread.sleep(1000)
    }
  }
}

object MockStream {
  val OUTPUT_NAME = "output"
}
