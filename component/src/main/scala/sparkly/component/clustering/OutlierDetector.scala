package sparkly.component.clustering

import sparkly.core._
import org.apache.spark.streaming.dstream.DStream
import sparkly.core.OutputStreamMetadata
import sparkly.core.Context
import sparkly.core.PropertyMetadata
import scala.Some
import sparkly.core.InputStreamMetadata
import sparkly.math.clustering.{ClusterOutlierDetector, StreamingKMeansPP}
import org.apache.spark.mllib.linalg.VectorUtil._

class OutlierDetector extends Component {

  override def metadata: ComponentMetadata = ComponentMetadata (
    name = "Outlier detector", category = "Clustering",
    description =
      """
        | Cluster based outlier detector.
      """.stripMargin,
    inputs = Map (
      "Input" -> InputStreamMetadata(namedFeatures = Map("Features" -> FeatureType.VECTOR))
    ),outputs = Map (
      "Output" -> OutputStreamMetadata(from = Some("Input"), namedFeatures = Map("Outlier" -> FeatureType.BOOLEAN))
    ), properties = Map (
      "Alpha" -> PropertyMetadata(PropertyType.DECIMAL, defaultValue = Some(0.90), description = "Estimated part of the outlier data"),
      "Beta" -> PropertyMetadata(PropertyType.DECIMAL, mandatory = false),
      "Clusters" -> PropertyMetadata(PropertyType.INTEGER, defaultValue = Some(2), description = "Number of clusters used to model behavior."),
      "Decay factor" -> PropertyMetadata(PropertyType.DECIMAL, defaultValue = Some(1.0), description = "Decay factor")
    )
  )

  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val alpha = context.property("Alpha").as[Double]
    val beta = context.property("Beta").asOption[Double]
    val outlierDetector = ClusterOutlierDetector(alpha = alpha, beta = beta, k = None)

    val k = context.property("Clusters").as[Int]
    val decayFactor = context.property("Decay factor").as[Double]
    val model = new StreamingKMeansPP()
      .setK(k)
      .setDecayFactor(decayFactor)

    val output = context.dstream("Input", "Output").transform{ instances =>
      if(!instances.isEmpty()) {
        val data = instances.map(_.inputFeature("Features").asVector.toDenseSpark)

        model.update(data)
        val assignments = model.latestModel().predict(data)

        val centers = model.modelCenters()
        val weights = model.modelWeights()
        val outliers = outlierDetector.findOutlierClusters(centers, weights)

        instances.zip(assignments).map{case (instance, cluster) =>
          instance.outputFeature("Outlier", outliers.contains(cluster))
        }
      } else {
        instances.sparkContext.emptyRDD[Instance]
      }
    }

    Map("Output" -> output)
  }

}
