package sparkly.component.nlp

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Milliseconds
import org.apache.spark.streaming.dstream.DStream
import scala.Some
import sparkly.core._
import sparkly.core.PropertyType._

class TopKWords extends Component {

  override def metadata: ComponentMetadata = ComponentMetadata (
    name = "Top K words", category = "Text processing",
    description =
      """
        |Find the most frequent words over a sliding window.
      """.stripMargin,
    inputs = Map("Input" -> InputStreamMetadata(namedFeatures = Map("Text" -> FeatureType.STRING))),
    outputs = Map("Output" -> OutputStreamMetadata(namedFeatures = Map("Word" -> FeatureType.STRING, "Rank" -> FeatureType.INTEGER, "Count" -> FeatureType.LONG))),
    properties = Map (
      "K" -> PropertyMetadata(INTEGER, defaultValue = Some(10)),
      "Tokenize" -> PropertyMetadata(BOOLEAN, defaultValue = Some(true), description = "Whether to tokenize input text or consider it as a steam of words"),
      "Tokenizer language" -> PropertyMetadata(STRING, defaultValue = Some("English"), mandatory = false, description = "Language used to extract words", acceptedValues = AnalyzerFactory.availableLanguages),
      "Window length (in ms)" -> PropertyMetadata(LONG, mandatory = false, description = "Windows size (in ms). Leave empty to use pipeline's window"),
      "Parallelism" -> PropertyMetadata(INTEGER, mandatory = false, description = "Level of parallelism to use. Leave empty to use default parallelism.")
    )
  )

  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val k = context.property("K").as[Int]
    val windowLengthMs = context.properties( "Window length (in ms)").asOption[Long]
    val parallelism = context.property("Parallelism").or(context.sc.defaultParallelism)

    // Extract words
    val rawTexts = context.dstream("Input").map(_.inputFeature("Text").asString)
    val words = context.property("Tokenize").as[Boolean] match {
      case false => rawTexts
      case true => {
        val language = context.property("Tokenizer language").as[String]
        val tokenizer = TextTokenizer(language)
        rawTexts.flatMap(text => tokenizer.tokenize(text))
      }
    }

    // Count words
    val wordCounts = windowLengthMs match {
      case None => words.countByValue(numPartitions = parallelism)
      case Some(ms) => {
        val slideDuration = words.slideDuration
        val windowDuration = Milliseconds(ms)
        words.countByValueAndWindow(windowDuration = windowDuration, slideDuration = slideDuration, numPartitions = parallelism)
      }
    }

    // Top K
    val output = wordCounts.transform{ counts: RDD[(String, Long)] =>
      val tops = counts.top(k)(Ordering.by[(String, Long), Long](_._2))
      val instances = tops.zipWithIndex.map{ case ((word, count), rank) => Instance("Word" -> word, "Rank" -> rank, "Count" -> count)}
      counts.sparkContext.makeRDD(instances, numSlices = 1)
    }

    Map("Output" -> output)
  }
}

