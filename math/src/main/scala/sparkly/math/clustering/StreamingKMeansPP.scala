package sparkly.math.clustering

import org.apache.spark.mllib.clustering.{StreamingKMeansModel, StreamingKMeans}
import org.apache.spark.mllib.linalg._
import org.apache.spark.streaming.dstream.DStream
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import org.apache.spark.rdd.RDD
import sparkly.math.stat.RandomWeighted

class StreamingKMeansPP extends StreamingKMeans {

  override def trainOn(data: DStream[Vector]) {
    data.foreachRDD(rdd => update(rdd))
  }

  def update(data: RDD[Vector]): Unit = {
    if (!data.isEmpty) {
      // KMeans++ init
      if (model.clusterCenters == null) {
        val samples = data.collect()
        val centers = initCentroids(samples)
        setInitialCenters(centers, Array.fill(k)(0.0))
      }

      model = model.update(data, decayFactor, timeUnit)
    }
  }

  private def initCentroids(data: Array[Vector]): Array[Vector] = {
    val points = ArrayBuffer(data :_*)

    val centers = ArrayBuffer.empty[Vector]
    centers += points.remove(Random.nextInt(points.size))

    (1 until k).foreach{ index =>
      val dxs = points.map { point =>
        val nearestCentroid = centers.map(c => (c, Vectors.sqdist(c, point))).minBy(_._2)
        (point, nearestCentroid._2 * nearestCentroid._2)
      }

      val center = RandomWeighted(dxs).randomElement()
      points -= center
      centers += center
    }

    centers.toArray
  }

  def modelCenters(): Array[Vector] = model.clusterCenters
  def modelWeights(): Array[Double] = model.clusterWeights
}