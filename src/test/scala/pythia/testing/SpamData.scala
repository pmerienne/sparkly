package pythia.testing

import pythia.core._

import scala.io.Source

trait SpamData {

  def dataset = Source.fromFile("src/test/resources/spam.data")
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
