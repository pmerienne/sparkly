package sparkly.math.clustering

import org.apache.spark.mllib.linalg._

case class ClusterOutlierDetector(k: Int, alpha: Double, beta: Int) {
  require(k > 0)
  require(0.0 < alpha && alpha < 1.0)
  require(beta > 1)

  def findOutlierClusters(centers: Array[Vector], weight: Array[Double]): Set[Int] = {
    val clusters = (centers zip weight).zipWithIndex.map{case ((center, weight), index) => ClusterInfo(index, center, weight)}
    val size = clusters.size
    val sortedClusters = clusters.sortBy(_.weight).reverse

    // Check alpha rule
    val d = firstOutlierIndex(sortedClusters)
    if(d < 0 || d >= size)
      return Set()

    val normalClusters = sortedClusters.slice(0, d)
    val outlierClusters = sortedClusters.slice(d, size)

    // Check beta rule
    val normalClustersMeanSize = normalClusters.map(_.weight).sum / d.toDouble
    val outlierClustersMeanSize = outlierClusters.map(_.weight).sum / (size - d).toDouble
    if(normalClustersMeanSize / outlierClustersMeanSize < beta)
      return Set()

    // Get K best outliers
    val topOutliers = outlierClusters
      .map(outlier => (outlier.index, degree(outlier, normalClusters)))
      .sortBy(_._2).reverse
      .take(k)
      .map(_._1)

    // re-map to non sorted clusters ...
    topOutliers.toSet
  }

  private def firstOutlierIndex(sortedClusters: Array[ClusterInfo]): Int = {
    val runningWeightSums = sortedClusters.map(_.weight).scanLeft(0.0)(_ + _)
    val bound = alpha * runningWeightSums.last
    runningWeightSums.indexWhere(sum => sum >= bound)
  }

  private def degree(outlier: ClusterInfo, normalClusters: Array[ClusterInfo]) = {
    minDistance(outlier, normalClusters) / outlier.weight
  }

  private def minDistance(cluster: ClusterInfo, others: Array[ClusterInfo]): Double = {
    others.map(other => Vectors.sqdist(other.center, cluster.center)).min
  }
}

case class ClusterInfo(index: Int, center: Vector, weight: Double)