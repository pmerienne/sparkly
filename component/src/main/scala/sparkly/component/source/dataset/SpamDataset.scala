package sparkly.component.source.dataset

import sparkly.core.FeatureType._
import sparkly.core.Instance
import scala.io.Source

class SpamDataset extends DatasetSource {

  override def metadata = super.metadata.copy (
    name = "Spam", category = "Dataset", description =
      """
        |Spam dataset containing a collection of spam and non-spam emails.
        |Data is encoded into 57 continuous features.
        |
      """.stripMargin
  )

  def file: String = SpamDataset.file
  def features: List[(String, FeatureType)] = (SpamDataset.labelName -> BOOLEAN) :: SpamDataset.featureNames.map(name => (name, DOUBLE))

}

object SpamDataset {
  val file: String = "/dataset/spam.csv"
  val labelName = "Label"
  val featureNames = List.range(1, 56).map(index => (s"f${index}"))
  val labelAndFeatures = labelName :: featureNames

  def iterator(): Iterator[Instance] = Source.fromInputStream(getClass.getResourceAsStream(file)).getLines().map{ line =>
    val values = (labelAndFeatures, line.split(",").map(_.toDouble)).zipped.toMap
    Instance(values)
  }
}