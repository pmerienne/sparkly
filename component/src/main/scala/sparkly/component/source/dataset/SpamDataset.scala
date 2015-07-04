package sparkly.component.source.dataset

import sparkly.core.FeatureType._

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
  val file: String = "spam.data"
  val labelName = "Label"
  val featureNames = List.range(1, 56).map(index => (s"f${index}"))
}