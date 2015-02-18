package pythia.testing.kafka

import kafka.server.{KafkaConfig, KafkaServerStartable}
import org.apache.curator.test.{InstanceSpec, QuorumConfigBuilder, TestingZooKeeperServer}
import kafka.utils.TestUtils
import scala.util.Random
import kafka.producer.{KeyedMessage, ProducerConfig, Producer}
import java.util.Properties

class EmbeddedZkKafkaCluster(val kafkaPort: Int = 9092, val zkPort: Int = 2181) {

  val zkServer = new TestingZooKeeperServer(EmbeddedZkKafkaCluster.getZkConfig(zkPort))
  val kafkaServer = new KafkaServerStartable(EmbeddedZkKafkaCluster.getKafkaConfig(kafkaPort, zkServer.getInstanceSpec.getConnectString))
  val producer = new Producer[String, String](EmbeddedZkKafkaCluster.getKafkaProducerConfig(kafkaBroker, zkServer.getInstanceSpec.getConnectString))

  def startZkKafkaCluster() {
    zkServer.start()
    kafkaServer.startup()
  }

  def stopZkKafkaCluster() {
    kafkaServer.shutdown()
    zkServer.stop()
    kafkaServer.awaitShutdown()
  }

  def kafkaBroker = s"localhost:${kafkaPort}"
  def zkConnectString = zkServer.getInstanceSpec.getConnectString

  def sendMessage(topic: String, key: String, message: String) {
    val keyedMessage = new KeyedMessage[String, String](topic, key, message)
    producer.send(keyedMessage)
  }

}

object EmbeddedZkKafkaCluster {

  def getKafkaConfig(kafkaPort: Int, zkConnectString: String): KafkaConfig = {
    val props = TestUtils.createBrokerConfigs(1).iterator.next
    props.put("zookeeper.connect", zkConnectString)
    props.put("port", kafkaPort.toString)
    props.put("auto.create.topics.enable", "true")
    new KafkaConfig(props);
  }

  def getKafkaProducerConfig(kafkaBroker: String, zkConnect: String): ProducerConfig = {
    val props = new Properties();
    props.put("metadata.broker.list", kafkaBroker)
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("message.send.max.retries", "5")
    props.put("retry.backoff.ms", "500")
    new ProducerConfig(props);
  }

  def getZkConfig(zkPort: Int): QuorumConfigBuilder = {
    val spec = new InstanceSpec(null, zkPort, -1, -1, true, -1)
    new QuorumConfigBuilder(spec)
  }

  def randomPort(min: Int, max: Int): Int = {
    val range = min to max
    range(Random.nextInt(range length))
  }
}
