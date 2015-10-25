package sparkly.math.clustering

import org.scalatest._
import org.apache.spark.mllib.linalg._

class ClusterOutlierDetectorSpec extends FlatSpec with Matchers {

  "ClusterOutlierDetector" should "detected outlier clusters" in {
    // Given
    val detector = ClusterOutlierDetector(alpha = 0.95)
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

  "ClusterOutlierDetector" should "detected only top outlier clusters by limiting number of outlier cluster" in {
    // Given
    val detector = ClusterOutlierDetector(alpha = 0.95, k = Some(1))
    val centers = Array (
      Vectors.dense(6.0, 5.0),
      Vectors.dense(8.0, 9.0),
      Vectors.dense(5.0, 5.0),
      Vectors.dense(1.0, 3.0),
      Vectors.dense(1.0, 1.0)
    )

    val weights = Array(42.0, 28.0, 26.0, 3.0, 2.0)

    // When
    val outliers = detector.findOutlierClusters(centers, weights)

    // Then
    outliers should contain only (4)
  }

  "ClusterOutlierDetector" should "detected only top outlier clusters using a minimum distance" in {
    // Given
    val detector = ClusterOutlierDetector(alpha = 0.95, beta = Some(0.5))
    val centers = Array (
      Vectors.dense(6.0, 5.0),
      Vectors.dense(8.0, 9.0),
      Vectors.dense(5.0, 5.0),
      Vectors.dense(1.0, 3.0),
      Vectors.dense(1.0, 1.0)
    )

    val weights = Array(42.0, 28.0, 26.0, 3.0, 2.0)

    // When
    val outliers = detector.findOutlierClusters(centers, weights)

    // Then
    outliers should contain only (4)
  }

}
