package pythia.component

import java.io.{File, FileWriter}

import pythia.core._
import pythia.testing.InspectedStream

class CsvSourceSpec extends ComponentSpec {

  "CsvSource" should "stream .csv file" in {
    write("/tmp/data.csv", List("Julie;1981;Female", "Pierre;1987;Male"))

    val configuration = ComponentConfiguration (
      clazz = classOf[CsvSource].getName,
      name = "Train data",
      properties = Map (
        "File" -> "/tmp/data.csv"
      ),
      outputs = Map (
        "Instances" -> StreamConfiguration(selectedFeatures = Map("Features" -> List("Name", "Birth date", "Gender")))
      )
    )

    val outputs: Map[String, InspectedStream] = deployComponent(configuration, Map())

    eventually {
      outputs("Instances").features should contain only (
        Map("Name" -> "Julie", "Birth date" -> "1981", "Gender" -> "Female"),
        Map("Name" -> "Pierre", "Birth date" -> "1987", "Gender" -> "Male")
      )
    }
  }

  def write(filename: String, values: List[String]) {
    val file = new File(filename)
    file.delete()

    val writer = new FileWriter(file)
    values.foreach(line => writer.write(s"${line}\n"))
    writer.close()
  }


  def append(filename: String, values: List[String]) {
    val writer = new FileWriter(filename, true)
    values.foreach(line => writer.write(s"${line}\n"))
    writer.close()
  }
}
