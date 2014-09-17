package pythia.component

import pythia.core._

class FileSource extends Component {
   def metadata = Metadata (
    "Text file source", "Read a file line by line",
    properties = List(PropertyMetadata("Filename", "STRING", mandatory = true)),
    outputStreams = List(StreamMetadata("Lines", namedFeatures = List("Line")))
   )
 }

class CsvFileSource extends Component {
  def metadata = Metadata (
    "Csv file source", "Read and parse a .csv line by line",
    properties = List(
      PropertyMetadata("Filename", "STRING", mandatory = true, defaultValue = "/tmp/default.txt"),
      PropertyMetadata("Some decimal", "DECIMAL", mandatory = true),
      PropertyMetadata("Some integer", "INTEGER", mandatory = true),
      PropertyMetadata("Fake boolean", "BOOLEAN"),
      PropertyMetadata("Separator", "ENUM", acceptedValues = List(";", ",", "Space"), mandatory = true)
    ),
    outputStreams = List(StreamMetadata("Lines", listedFeatures = List("Features")))
  )
}
