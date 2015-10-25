package sparkly.component.source


import java.util.Properties

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter._

import sparkly.core._
import sparkly.core.PropertyType._

import twitter4j.auth.OAuthAuthorization
import twitter4j.conf._

class TwitterSource extends Component {

  def metadata = ComponentMetadata (
    name = "Tweet source", description = "Use twitter as a stream source", category = "Twitter",
    properties = Map(
      "Consumer key" -> PropertyMetadata(STRING),
      "Consumer secret" -> PropertyMetadata(STRING),
      "Access token" -> PropertyMetadata(STRING),
      "Access token secret" -> PropertyMetadata(STRING),
      "Keywords" -> PropertyMetadata(STRING, description = "Comma-separated list of keywords to track", mandatory = false)
    ),
    outputs = Map(
      "Tweets" -> OutputStreamMetadata(namedFeatures = Map (
        "User id" -> FeatureType.LONG,
        "Text" -> FeatureType.STRING,
        "Lang" -> FeatureType.STRING,
        "Size" -> FeatureType.INTEGER
      ))
    )
  )

  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val keywords = context.property("Keywords").or("").split(",").map(_.trim).filter(!_.isEmpty).toSeq

    val properties = new Properties()
    properties.put("oauth.consumerKey", context.property("Consumer key").as[String])
    properties.put("oauth.consumerSecret", context.property("Consumer secret").as[String])
    properties.put("oauth.accessToken", context.property("Access token").as[String])
    properties.put("oauth.accessTokenSecret", context.property("Access token secret").as[String])

    val auth = new OAuthAuthorization(new PropertyConfiguration(properties))

    val tweets = TwitterUtils
      .createStream(context.ssc, Some(auth), filters = keywords)
      .map(status => Instance(
        "User id" -> status.getUser.getId,
        "Text" -> status.getText,
        "Lang" -> status.getUser.getLang,
        "Size" -> status.getText.size
      ))

    Map("Tweets" -> tweets)
  }
}
