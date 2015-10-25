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
    inputs = Map (
      "Train" -> InputStreamMetadata(namedFeatures = Map("Features" -> FeatureType.VECTOR)),
      "Predict" -> InputStreamMetadata(namedFeatures = Map("Features" -> FeatureType.VECTOR))
    ),outputs = Map (
      "Predicted" -> OutputStreamMetadata(from = Some("Predict"), namedFeatures = Map("Cluster" -> FeatureType.INTEGER))
    ), properties = Map (
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

    val train = context.dstream("Train").map(i => i.inputFeature("Features").asVector.toDenseSpark).cache()

    // Train
    model.trainOn(train)

    // Predict
    val predictions = context.dstream("Predict", "Predicted").map{ i =>
      val features = i.inputFeature("Features").asVector.toSpark
      val cluster = model.latestModel().predict(features)
      i.outputFeature("Cluster", cluster)
    }

    Map("Predicted" -> predictions)
  }

}