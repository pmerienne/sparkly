package sparkly.testing

import sparkly.core._
import scala.io.Source
import org.scalatest.FlatSpec
import org.scalatest.Matchers

trait SpamData {

  def dataset = Source.fromInputStream(getClass.getResourceAsStream("/spam.data"))
    .getLines.toList
    .map(line => line.split(";").toList)
    .map(values => {
      val entries = values.zipWithIndex
        .map { case (value, index) => s"f${index}" -> value}
        .toMap
      Instance(entries)
    })

  def featureNames = List.range(1, 56).map(index => s"f${index}")
  def labelName = "f0"
}

class SpamDataSpec extends FlatSpec with Matchers with SpamData {
  
  "Spam data" should "load spam.data" in {
    dataset.size should be > 0
  }
}