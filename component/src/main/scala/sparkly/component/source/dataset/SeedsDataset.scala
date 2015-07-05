package sparkly.component.source.dataset

import sparkly.core.FeatureType._
import sparkly.core.Instance
import scala.io.Source

class SeedsDataset extends DatasetSource {

  override def metadata = super.metadata.copy (
    name = "Seeds", category = "Dataset", description =
      """
        |Measurements of geometrical properties of kernels belonging to three different varieties of wheat.
        |A soft X-ray technique and GRAINS package were used to construct all seven, real-valued attributes.
        |The examined group comprised kernels belonging to three different varieties of wheat: Kama, Rosa and Canadian, 70 elements each.
        |
        |The data set can be used for the tasks of classification and cluster analysis.
        |
      """.stripMargin
  )

  def file: String = SeedsDataset.file
  def features: List[(String, FeatureType)] = SeedsDataset.featureNames.map(name => (name, DOUBLE)) :+ (SeedsDataset.labelName -> INTEGER)

}

object SeedsDataset {
  val file = "/dataset/seeds.csv"
  val labelName = "Variety"
  val featureNames = (0 to 6).map(index => (s"f${index}")).toList
  val labelAndFeatures = featureNames :+ labelName

  def iterator(): Iterator[Instance] = Source.fromInputStream(getClass.getResourceAsStream(file)).getLines().map{ line =>
    val values = (labelAndFeatures, line.split(",").map(_.toDouble)).zipped.toMap
    Instance(values)
  }
}