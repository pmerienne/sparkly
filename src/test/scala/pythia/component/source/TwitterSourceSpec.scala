package pythia.component.source

import pythia.component.ComponentSpec

class TwitterSourceSpec extends ComponentSpec {

  /*
  "TwitterSource" should "stream tweets" in {
    val outputs: Map[String, InspectedStream] = deployComponent(ComponentConfiguration (
      clazz = classOf[TwitterSource].getName,
      name = "Tweets soure",
      properties = Map (
        "Consumer key" -> System.getenv("TWITTER_OAUTH_CONSUMER_KEY"),
        "Consumer secret" -> System.getenv("TWITTER_OAUTH_CONSUMER_SECRET"),
        "Access token" -> System.getenv("TWITTER_OAUTH_ACCESS_TOKEN"),
        "Access token secret" -> System.getenv("TWITTER_OAUTH_ACCESS_TOKENS_SECRET")
      ),
      outputs = Map (
        "Tweets" -> StreamConfiguration(mappedFeatures = Map("User id" -> "User id", "Text" -> "Text"))
      )
    ), Map())

    eventually {
      outputs("Tweets").count() should be > 0
    }
  }
  */
}
