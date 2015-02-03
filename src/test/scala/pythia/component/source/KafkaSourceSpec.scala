package pythia.component.source

import pythia.component.ComponentSpec
import pythia.core.{ComponentConfiguration, StreamConfiguration}
import pythia.testing.kafka.EmbeddedZkKafkaCluster

class KafkaSourceSpec extends ComponentSpec {

  var embeddedZkKafkaCluster: EmbeddedZkKafkaCluster = _

  override def beforeEach() {
    super.beforeEach()
    embeddedZkKafkaCluster = new EmbeddedZkKafkaCluster()
    embeddedZkKafkaCluster.startZkKafkaCluster()
  }

  override def afterEach() {
    super.afterEach()
    embeddedZkKafkaCluster.stopZkKafkaCluster()
  }

  "Kafka source" should "stream message from a topic" in {
    // Given
    val messages = List("Hello", "How are you?", "Does it work?")

    val configuration = ComponentConfiguration (
      clazz = classOf[KafkaSource].getName,
      name = "Kafka source",
      properties = Map (
        "Topic" -> "test-topic",
        "Group Id" -> "kafka-source-test",
        "Consumers threads" -> "2",
        "Zookeeper quorum" -> embeddedZkKafkaCluster.zkConnectString,
        "Storage level" -> "Memory Only"
      ),
      outputs = Map (
        "Output" -> StreamConfiguration(mappedFeatures = Map("Message" -> "Message"))
      )
    )

    // When
    val component = deployComponent(configuration)
    Thread.sleep(1000)
    messages.foreach(message => embeddedZkKafkaCluster.sendMessage("test-topic", "key", message))

    // Then
    eventually {
      component.outputs("Output").features should contain only (
        Map("Message" -> "Hello"),
        Map("Message" -> "How are you?"),
        Map("Message" -> "Does it work?")
      )
    }
  }

}
