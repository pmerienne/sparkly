package sparkly.testing

import sparkly.core._
import scala.io.Source
import org.scalatest.FlatSpec
import org.scalatest.Matchers

trait SpamData {
  def dataset() = SpamData.getDataset()
  def featureNames = List.range(1, 56).map(index => s"f${index}")
  def labelName = "f0"
}

object SpamData {

  private var dataset: Option[List[Instance]] = None

  def getDataset(): List[Instance] = dataset match {
    case Some(data) => data
    case None => loadDataset()
  }

  def loadDataset(): List[Instance] = {
    dataset = Some(Source.fromInputStream(getClass.getResourceAsStream("/spam.data"))
      .getLines.toList
      .map(line => line.split(";").toList)
      .map(values => {
      val entries = values.zipWithIndex
        .map { case (value, index) => s"f${index}" -> value}
        .toMap
      Instance(entries)
    }))

    dataset.get
  }
}

class SpamDataSpec extends FlatSpec with Matchers with SpamData {
  
  "Spam data" should "load spam.data" in {
    dataset.size should be > 0
  }
}