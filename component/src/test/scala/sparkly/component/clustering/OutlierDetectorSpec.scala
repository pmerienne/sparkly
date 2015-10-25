package sparkly.component.clustering

import sparkly.testing.ComponentSpec
import sparkly.core.{Instance, StreamConfiguration, ComponentConfiguration}
import scala.util.Random
import breeze.linalg.DenseVector
import sparkly.math.stat.RandomWeighted

class OutlierDetectorSpec extends ComponentSpec {

  "OutlierDetector" should "detect outliers" in {
    // Given
    val configuration = ComponentConfiguration (
      name = "OutlierDetector",
      clazz = classOf[OutlierDetector].getName,
      inputs = Map ("Input" -> StreamConfiguration(mappedFeatures = Map("Features" -> "Features"))),
      outputs = Map("Output" -> StreamConfiguration(mappedFeatures = Map("Outlier" -> "Outlier"))),
      properties = Map ("Clusters" -> "4", "Alpha" -> "0.90")
    )

    // When
    val component = deployComponent(configuration)
    val generator = ClusterDataGenerator(List (
      Centroid("Cluster 1", 0.3, Array(2.0, 2.0)),
      Centroid("Cluster 2", 0.3, Array(1.0, 2.0)),
      Centroid("Cluster 2", 0.3, Array(2.0, 1.0)),
      Centroid("I'm an outlier", 0.05, Array(0.0, 0.0))
    ))

    component.inputs("Input").push(2000, generator.generate _)

    // Then
    eventually {
      val all = component.outputs("Output").features.takeRight(1000)

      val outliers = all.filter(f => f("Name") == "I'm an outlier")
      val truePositives = outliers.filter(f => f("Outlier") == true).size.toDouble / outliers.size.toDouble

      val normals = all.filter(f => f("Name") != "I'm an outlier")
      val falsePositives = normals.filter(f => f("Outlier") == true).size.toDouble / normals.size.toDouble

      println(s"truePositives: $truePositives, falsePositives: $falsePositives")

      truePositives should be > 0.90
      falsePositives should be < 0.01
    }
  }

}

case class ClusterDataGenerator(centroids: List[Centroid]) {
  val random = RandomWeighted(centroids.map(c => (c, c.weight)))

  def generate(): Instance = {
    val centroid = random.randomElement()
    centroid.generate()
  }

}

case class Centroid(name: String, weight: Double, center: Array[Double]) {
  def generate(): Instance = {
    val features = (0 until center.length).map(i => center(i) + (Random.nextGaussian() * 0.5)).toArray
    Instance("Name" -> name, "Features" -> DenseVector(features))
  }
}