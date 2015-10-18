package sparkly.component.nlp

import collection.JavaConversions.enumerationAsScalaIterator
import java.io.{FileOutputStream, File}
import java.net.URL
import java.util.UUID
import java.util.zip.ZipFile
import org.apache.commons.lang.StringEscapeUtils
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.classification.{NaiveBayesModel, NaiveBayes}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.VectorUtil._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import sparkly.component.utils.StorageUtils
import sparkly.core._
import sys.process._

class TwitterSentimentClassifier extends Component {

  override def metadata: ComponentMetadata = ComponentMetadata (
    name = "Twitter Sentiment Classifier", category = "Twitter",
    description =
      """
        |Sentiment classifier build on top of the Twitter Sentiment Corpus by Niek Sanders.
        |It classify english tweets as positive (1.0) or negative (0.0).
      """.stripMargin,
    inputs = Map ("Input" -> InputStreamMetadata(namedFeatures = Map("Tweet" -> FeatureType.STRING))),
    outputs = Map("Output" -> OutputStreamMetadata(from = Some("Input"), namedFeatures = Map("Sentiment" -> FeatureType.DOUBLE)))
  )


  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val path = this.getClass.getClassLoader.getResource("model/twitter-sentiment-classifier").getFile
    val model = context.sc.broadcast(TwitterSentimentClassifierModel.load(context.sc, path))

    val output = context.dstream("Input", "Output").map{ i =>
      val tweet = i.inputFeature("Tweet").asString
      val sentiment = model.value.predict(tweet)
      i.outputFeature("Sentiment", sentiment)
    }

    Map("Output" -> output)
  }
}

case class TwitterSentimentClassifierModel(tokenizer: TextTokenizer, tfIdfModel: TfIdfModel, naiveBayesModel: NaiveBayesModel) {

  def predict(tweet: String): Double = {
    val text = StringEscapeUtils.escapeHtml(tweet)
    val terms = tokenizer.tokenize(text)
    val features = tfIdfModel.tfIdf(terms).toSpark
    naiveBayesModel.predict(features)
  }

  def save(sc: SparkContext, directory: String): Unit = {
    // Save tokenizer
    StorageUtils.save(sc, new Path(directory, "tokenizer").toUri.toString, tokenizer)

    // Save idf-data
    StorageUtils.save(sc, new Path(directory, "tf-idf").toUri.toString, tfIdfModel)

    // Save classification model
    naiveBayesModel.save(sc, new Path(directory, "naive-bayes").toUri.toString)
  }
}

object TwitterSentimentClassifierModel {

  def load(sc: SparkContext, directory: String): TwitterSentimentClassifierModel = {
    // Load tokenizer
    val tokenizer = StorageUtils.load[TextTokenizer](sc, new Path(directory, "tokenizer").toUri.toString)

    // Load tf-idf
    val tfIdfModel = StorageUtils.load[TfIdfModel](sc, new Path(directory, "tf-idf").toUri.toString)

    // Load classification model
    val naiveBayesModel = NaiveBayesModel.load(sc, new Path(directory, "naive-bayes").toUri.toString)

    TwitterSentimentClassifierModel(tokenizer, tfIdfModel, naiveBayesModel)
  }
}

object TwitterSentimentClassifierModelCreator {

  val cores = 4
  val conf = new SparkConf().setMaster(s"local[$cores]").setAppName(getClass.getName)
  val sc = new SparkContext(conf)

  val tokenizer =  TextTokenizer(language = "English", minNGram = 2, maxNGram = 2,
    ignorePatterns = List("((www\\.[\\s]+)|(https?://[^\\s]+))", "[1-9](\\w+)*", "@([^\\s]+)", "&amp;"),
    replacePatterns = Map("([a-z])\\1{1,}" -> "$1")
  )

  def main(args: Array[String]) {
    val destination = "/tmp/tscm-" + UUID.randomUUID.toString

    val models = for(vocabularySize <- 5000 to 100000 by 5000; lambda <- 0.0 to 1.0 by 0.5) yield {evaluate(vocabularySize, lambda)}
    val bestModel = models.maxBy(_._2.areaUnderROC())
    bestModel._1.save(sc, destination)

    println(s"Best model found with Area under ROC: ${bestModel._2.areaUnderROC()} and Area under precision-recall: ${bestModel._2.areaUnderPR()}")
    println("Model saved to " + destination)
  }

  def evaluate(vocabularySize: Int, lambda: Double): (TwitterSentimentClassifierModel, BinaryClassificationMetrics) = {
    val (training, testing) = datasets()
    val model = train(training, vocabularySize, lambda)
    val results = test(model, testing)
    println(s"Vocabulary size: $vocabularySize, Lambda: $lambda => Area under ROC: ${results.areaUnderROC()} and Area under precision-recall: ${results.areaUnderPR()}")
    (model, results)
  }

  def train(rawData: RDD[(Double, scala.List[String])], vocabularySize: Int, lambda: Double): TwitterSentimentClassifierModel = {
    // Create IDF model
    val tfIdfModel = rawData.aggregate(TfIdfModel(vocabularySize))(
      seqOp = (model, datum) => model.add(datum._2),
      combOp = _ + _
     )

    // Train classification model
    val data = rawData.map{ case (sentiment, terms) => LabeledPoint(sentiment, tfIdfModel.tfIdf(terms).toSpark)}.cache()
    val classificationModel = NaiveBayes.train(data, lambda = lambda, modelType = "multinomial")
    data.unpersist()

    TwitterSentimentClassifierModel(tokenizer, tfIdfModel, classificationModel)
  }

  def test(model: TwitterSentimentClassifierModel, testDataset: RDD[(Double, String)]): BinaryClassificationMetrics = {
    val scoreAndLabels = testDataset.map{case (sentiment, tweet) =>
      val prediction = model.predict(tweet)
      (prediction, sentiment)
    }

    new BinaryClassificationMetrics(scoreAndLabels)
  }

  def datasets(): (RDD[(Double, scala.List[String])], RDD[(Double, String)]) = {
    val (training, testing) = datasetsPaths()

    // Load training data
    val trainData = sc.textFile(training).map{line =>
      val values = line.split(",").map(value => value.replaceAll("\"", ""))
      val sentiment = values(0).toInt.toDouble / 4.0

      val tweet = values(5)
      val text = StringEscapeUtils.escapeHtml(tweet)
      val terms = tokenizer.tokenize(text)

      (sentiment, terms)
    }.filter(_._1 != 0.5).cache()

    val testData = sc.textFile(testing).map{line =>
      val values = line.split(",").map(value => value.replaceAll("\"", ""))
      val tweet = values(5)
      val sentiment = values(0).toInt.toDouble / 4.0

      (sentiment, tweet)
    }.filter(_._1 != 0.5).cache()

    (trainData, testData)
  }

  def datasetsPaths(): (String, String) = {
    val trainFile = "/tmp/niek-sanders-tweets-train.csv"
    val testFile = "/tmp/niek-sanders-tweets-test.csv"
    if(!new File(trainFile).exists || !new File(testFile).exists) {
      downloadDatasets()
    }
    (trainFile, testFile)
  }

  def downloadDatasets() = {
    val url = "http://cs.stanford.edu/people/alecmgo/trainingandtestdata.zip"
    println(s"Downloading $url")
    val zipDestination = s"/tmp/trainingandtestdata.zip"
    new URL(url).#>(new File(zipDestination)).!!

    val destinations = Map(
      "training.1600000.processed.noemoticon.csv" -> "/tmp/niek-sanders-tweets-train.csv",
      "testdata.manual.2009.06.14.csv" -> "/tmp/niek-sanders-tweets-test.csv"
    )

    println(s"Unzipping $zipDestination")
    val zipFile = new ZipFile(zipDestination)
    zipFile.entries().foreach { entry =>
      val destination = destinations(entry.getName)
      IOUtils.copy(zipFile.getInputStream(entry), new FileOutputStream(destination))
    }
  }
}