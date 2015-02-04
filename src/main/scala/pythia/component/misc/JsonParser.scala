package pythia.component.misc

import java.util

import io.gatling.jsonpath.JsonPath
import org.apache.spark.streaming.dstream.DStream
import org.codehaus.jackson.map.ObjectMapper
import pythia.core._

import scala.util.Try

class JsonParser extends Component {

  override def metadata: ComponentMetadata = ComponentMetadata(
    name = "JSON parser", category = "Miscellaneous",
    description = "Parse a JSON string to extract",
    properties = Map("Queries" -> PropertyMetadata(PropertyType.STRING, description = "Semicolon separated list of json-path expressions (see http://goessner.net/articles/JsonPath/)")),
    inputs = Map("Input" -> InputStreamMetadata(namedFeatures = Map("JSON" -> FeatureType.STRING))),
    outputs = Map("Output" -> OutputStreamMetadata(listedFeatures = Map("Values" -> FeatureType.STRING)))
  )

  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val queries = context.property("Queries").as[String].split(";")

    val output = context.dstream("Input", "Output").map { instance =>
      val values = JsonParser.extractValues(instance.inputFeature("JSON").as[String], queries)
      instance.outputFeatures("Values", values)
    }

    Map("Output" -> output)
  }
}

object JsonParser {
  def extractValues(json: String, queries: Iterable[String]): List[String] = {
      val mapper = new ObjectMapper
      val jsonObject = mapper.readValue(json, classOf[Object])

      val results = queries.map { q => Try {
        JsonPath.query(q, jsonObject).right.get.toList.head match {
          case col: util.Collection[_] => mapper.writeValueAsString(col)
          case map: util.Map[_, _] => mapper.writeValueAsString(map)
          case anythingElse => anythingElse.toString
        }
      }.getOrElse(null)
    }

    results.toList
  }
}