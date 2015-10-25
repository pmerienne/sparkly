package sparkly.math.clustering

import org.apache.spark.mllib.linalg._

case class ClusterOutlierDetector(alpha: Double, beta: Option[Double] = None, k: Option[Int] = None) {
  require(0.0 < alpha && alpha < 1.0)

  def findOutlierClusters(centers: Array[Vector], weights: Array[Double]): Set[Int] = {
    val clusters = (centers zip weights).zipWithIndex.map{case ((center, weight), index) => ClusterInfo(index, center, weight)}

    val (normalClusters, outlierCandidates) = alphaSplit(clusters)
    val outlierClusters = betaFilter(normalClusters, outlierCandidates)

    // Get best outliers
    val outliers_count = k.getOrElse(outlierClusters.size)
    val topOutliers = outlierClusters
      .map(outlier => (outlier.index, minDistance(outlier, normalClusters) / outlier.weight))
      .sortBy(_._2).reverse
      .map(_._1)
      .take(outliers_count)

    topOutliers.toSet
  }

  private def alphaSplit(clusters: Array[ClusterInfo]): (Array[ClusterInfo], Array[ClusterInfo]) = {
    val sortedClusters = clusters.sortBy(_.weight).reverse

    // Find split index
    val runningWeightSums = sortedClusters.map(_.weight).scanLeft(0.0)(_ + _)
    val bound = alpha * runningWeightSums.last
    val alphaIndex = runningWeightSums.indexWhere(sum => sum >= bound)

    if(alphaIndex < 0 || alphaIndex >= clusters.size)
      return (clusters, Array[ClusterInfo]())

    val normalClusters = sortedClusters.slice(0, alphaIndex)
    val outlierCandidates = sortedClusters.slice(alphaIndex, clusters.size)

    (normalClusters, outlierCandidates)
  }

  private def betaFilter(normalClusters: Array[ClusterInfo], outlierCandidates: Array[ClusterInfo]): Array[ClusterInfo] = if(beta.isEmpty) {
    outlierCandidates
  } else {
    val normalClustersMeanDistance = meanDistance(normalClusters)
    outlierCandidates.filter(cluster => minDistance(cluster, normalClusters) * beta.get > normalClustersMeanDistance)
  }

  private def minDistance(cluster: ClusterInfo, others: Array[ClusterInfo]): Double = {
    others.map(other => Vectors.sqdist(other.center, cluster.center)).min
  }

  private def meanDistance(clusters: Array[ClusterInfo]): Double = {
    val distances = clusters.flatMap{ cluster =>
      val others = clusters.filter(other => other.index != cluster.index)
      others.map(other => Vectors.sqdist(other.center, cluster.center))
    }

    distances.sum / distances.size
  }
}

case class ClusterInfo(index: Int, center: Vector, weight: Double)
