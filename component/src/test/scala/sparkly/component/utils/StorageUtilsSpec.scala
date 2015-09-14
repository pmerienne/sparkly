package sparkly.component.utils

import org.scalatest._
import org.apache.spark._
import java.util.UUID

class StorageUtilsSpec extends FlatSpec with Matchers {

  "StorageUtils" should "store case class" in {
    // Given
    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName(getClass.getName))
    val path = "/tmp/test-data-" + UUID.randomUUID.toString

    val expectedData = AnyData(
      3,
      List(0.4, 0.6),
      new org.apache.spark.mllib.linalg.SparseVector(10, Array(3, 5, 6), Array(0.4, 0.5, 0.9)),
      breeze.linalg.SparseVector[Double](0.4, 0.6, 0.8)
    )

    // When
    StorageUtils.save(sc, path, expectedData)
    val actualData = StorageUtils.load[AnyData](sc, path)

    // Then
    actualData should equal(expectedData)
  }

}

case class AnyData(someInt: Int, someList: List[Double], sparseVector: org.apache.spark.mllib.linalg.SparseVector, breezeVector: breeze.linalg.SparseVector[Double])
