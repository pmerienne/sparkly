package sparkly.math.clustering

import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg._
import org.apache.spark.streaming.dstream.DStream
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class StreamingKMeansPP extends StreamingKMeans {

  override def trainOn(data: DStream[Vector]) {
    data.foreachRDD { (rdd, time) => if (!rdd.isEmpty) {
      // KMeans++ init
      if (model.clusterCenters == null) {
        val data = rdd.collect()
        val centers = initCentroids(data)
        setInitialCenters(centers, Array.fill(k)(0.0))
      }

      model = model.update(rdd, decayFactor, timeUnit)
    }}
  }

  private def initCentroids(data: Array[Vector]): Array[Vector] = {
    val points = ArrayBuffer(data :_*)

    val centers = ArrayBuffer.empty[Vector]
    centers += points.remove(Random.nextInt(points.size))

    (1 until k).foreach{ index =>
      val dxs = points.map { point =>
        val nearestCentroid = centers.map(c => (c, Vectors.sqdist(c, point)))
        (point, nearestCentroid.maxBy(_._2)._2)
      }
      val farthest = dxs.maxBy(_._2)
      val center = dxs.find{ case (candidate, dx) => Random.nextDouble > (0.75 * Math.pow(dx / farthest._2, 2))}.getOrElse(farthest)._1
      points -= center
      centers += center
    }

    centers.toArray
  }

  def modelCenters(): Array[Vector] = model.clusterCenters
  def modelWeights(): Array[Double] = model.clusterWeights
}