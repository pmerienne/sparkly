package sparkly.component.source.dataset

import sparkly.core.FeatureType._
import sparkly.core._

import scala.io.Source

class HousingDataset extends DatasetSource {

  override def metadata = super.metadata.copy (
    name = "Housing", category = "Dataset", description =
      """
        |This dataset was taken from the StatLib library which is maintained at Carnegie Mellon University.
        |It concerns housing values in suburbs of Boston.
        |
        |The data set can be used for the tasks of regression.
        |For more information, see https://archive.ics.uci.edu/ml/datasets/Housing
      """.stripMargin
  )

  def file: String = HousingDataset.file
  def features: List[(String, FeatureType)] = HousingDataset.featureTypes

}

object HousingDataset {
  val file: String = "/dataset/housing.csv"
  val featureTypes = List (
    "CRIM" -> CONTINUOUS,
    "ZN" -> CONTINUOUS,
    "INDUS" -> CONTINUOUS,
    "CHAS" -> BOOLEAN,
    "NOX" -> CONTINUOUS,
    "RM" -> CONTINUOUS,
    "AGE" -> CONTINUOUS,
    "DIS" -> CONTINUOUS,
    "RAD" -> CONTINUOUS,
    "TAX" -> CONTINUOUS,
    "PTRATIO" -> CONTINUOUS,
    "B" -> CONTINUOUS,
    "LSTAT" -> CONTINUOUS,
    "MEDV" -> CONTINUOUS
  )
  val featureNames = featureTypes.map(_._1)

  def iterator(): Iterator[Instance] = Source.fromInputStream(getClass.getResourceAsStream(file)).getLines().map{ line =>
    val values = (featureTypes, line.split(",")).zipped.map{ case((name, featureType), raw) => featureType match {
      case CONTINUOUS => (name, raw.toDouble)
      case BOOLEAN => (name, raw == "1")
    }}.toMap
    Instance(values)
  }
}