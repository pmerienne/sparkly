package pythia.testing

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import pythia.core.Instance

import scala.collection.mutable

case class MockStream(ssc: StreamingContext) {
    val queue = new mutable.SynchronizedQueue[RDD[Instance]]()
    val dstream = ssc.queueStream(queue)

  def push(instances: Instance*): Unit = push(instances.toList)
  def push(instances: List[Instance]): Unit = queue += ssc.sparkContext.makeRDD(instances)
}
