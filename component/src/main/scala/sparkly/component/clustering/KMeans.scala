package sparkly.component.clustering

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.mllib.linalg.VectorUtil._
import sparkly.core._
import sparkly.math.clustering.StreamingKMeansPP

class KMeans extends Component {

  override def metadata: ComponentMetadata = ComponentMetadata (
    name = "KMeans", category = "Clustering",
    description =
      """
        |Clustering based on MLlib's KMeansModel with a Kmeans ++ initialization.
      """.stripMargin,
    inputs = Map ("Input" -> InputStreamMetadata(namedFeatures = Map("Features" -> FeatureType.VECTOR))),
    outputs = Map ("Output" -> OutputStreamMetadata(from = Some("Input"), namedFeatures = Map("Cluster" -> FeatureType.INTEGER))),
    properties = Map (
      "Clusters" -> PropertyMetadata(PropertyType.INTEGER, defaultValue = Some(2), description = "Number of clusters"),
      "Decay factor" -> PropertyMetadata(PropertyType.DECIMAL, defaultValue = Some(1.0), description = "Decay factor")
    )
  )

  // TODO : add kmeans cost monitoring
  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val k = context.properties("Clusters").as[Int]
    val decayFactor = context.properties("Decay factor").as[Double]

    val model = new StreamingKMeansPP()
      .setK(k)
      .setDecayFactor(decayFactor)

    val output = context.dstream("Input", "Output").transform{ instances =>
      if(!instances.isEmpty()) {
        val data = instances.map(_.inputFeature("Features").asVector.toDenseSpark)

        model.update(data)
        val assignments = model.latestModel().predict(data)


        instances.zip(assignments).map{case (instance, cluster) =>
          instance.outputFeature("Cluster", cluster)
        }
      } else {
        instances.sparkContext.emptyRDD[Instance]
      }
    }

    Map("Output" -> output)
  }

}