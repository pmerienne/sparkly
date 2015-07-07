package sparkly.component.source.dataset

import sparkly.core.FeatureType._
import sparkly.core.Instance

import scala.io.Source

class OptDigitsDataset extends DatasetSource {

  override def metadata = super.metadata.copy (
    name = "Optical digits", category = "Dataset", description =
      """
        |Preprocessed bitmaps of handwritten digits.
        |Data is encoded into 57 continuous features.
        |
        |The data set can be used for the tasks of multi-class classification.
        |For more information, see https://archive.ics.uci.edu/ml/datasets/Optical+Recognition+of+Handwritten+Digits
      """.stripMargin
  )

  def file: String = OptDigitsDataset.file
  def features: List[(String, FeatureType)] = OptDigitsDataset.featureNames.map(name => (name, INTEGER)) :+ (OptDigitsDataset.labelName -> CATEGORICAL)

}

object OptDigitsDataset {
  val file: String = "/dataset/optdigits.csv"
  val labelName = "Digit"
  val featureNames = (0 until 64).map(index => (s"f${index}")).toList
  val labelAndFeatures = featureNames :+ labelName

  def iterator(): Iterator[Instance] = Source.fromInputStream(getClass.getResourceAsStream(file)).getLines().map{ line =>
    val values = (labelAndFeatures, line.split(",").map(_.toInt)).zipped.toMap
    Instance(values)
  }
}