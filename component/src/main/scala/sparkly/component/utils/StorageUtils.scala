package sparkly.component.utils

import org.apache.spark.SparkContext
import scala.reflect.ClassTag

object StorageUtils {

  def save[T: ClassTag](sc: SparkContext, path: String, value: T): Unit = {
    sc.makeRDD(Seq(value), numSlices = 1).saveAsObjectFile(path)
  }

  def load[T: ClassTag](sc: SparkContext, path: String): T = {
    sc.objectFile[T](path, minPartitions = 1).collect().head
  }

}
