package sparkly.math.clustering

import org.scalatest._
import org.apache.spark.mllib.linalg._

class ClusterOutlierDetectorSpec extends FlatSpec with Matchers {

  "ClusterOutlierDetector" should "detected outlier clusters" in {
    // Given
    val detector = ClusterOutlierDetector(k = 2, alpha = 0.95, beta = 10)
    val centers = Array (
      Vectors.dense(6.0, 5.0),
      Vectors.dense(8.0, 9.0),
      Vectors.dense(5.0, 5.0),
      Vectors.dense(1.0, 3.0),
      Vectors.dense(3.0, 1.0)
    )

    val weights = Array(42.0, 28.0, 26.0, 3.0, 2.0)

    // When
    val outliers = detector.findOutlierClusters(centers, weights)

    // Then
    outliers should contain only (3, 4)
  }

  "ClusterOutlierDetector" should "not detected outlier clusters when beta rule not fulfill" in {
    // Given
    val detector = ClusterOutlierDetector(k = 2, alpha = 0.95, beta = 15)
    val centers = Array (
      Vectors.dense(6.0, 5.0),
      Vectors.dense(8.0, 9.0),
      Vectors.dense(5.0, 5.0),
      Vectors.dense(1.0, 3.0),
      Vectors.dense(3.0, 1.0)
    )

    val weights = Array(42.0, 28.0, 26.0, 3.0, 2.0)

    // When
    val outliers = detector.findOutlierClusters(centers, weights)

    // Then
    outliers.isEmpty should be (true)
  }

}
