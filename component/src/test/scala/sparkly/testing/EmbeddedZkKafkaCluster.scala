package sparkly.testing

import _root_.kafka.producer._
import _root_.kafka.server._
import _root_.kafka.utils.TestUtils
import kafka.server.KafkaConfig
import org.apache.curator.test._
import scala.util.Random
import kafka.producer.KeyedMessage
import java.util.Properties
import org.scalatest._
import kafka.consumer._
import kafka.serializer._
import java.util.concurrent.Executors
import scala.collection.mutable.ListBuffer

trait EmbeddedZkKafka extends FlatSpec with BeforeAndAfterEach {

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

}

class EmbeddedZkKafkaCluster(val kafkaPort: Int = 9092, val zkPort: Int = 2181) {

  val zkServer = new TestingZooKeeperServer(EmbeddedZkKafkaCluster.getZkConfig(zkPort))
  val kafkaServer = new KafkaServerStartable(EmbeddedZkKafkaCluster.getKafkaConfig(kafkaPort, zkServer.getInstanceSpec.getConnectString))
  val producer = new Producer[String, String](EmbeddedZkKafkaCluster.getKafkaProducerConfig(kafkaBroker, zkServer.getInstanceSpec.getConnectString))
  val connectors = ListBuffer[ConsumerConnector]()

  def startZkKafkaCluster() {
    zkServer.start()
    kafkaServer.startup()
  }

  def stopZkKafkaCluster() {
    connectors.foreach(_.shutdown())
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

  def listen(topic: String, groupId: String): ListBuffer[Array[Byte]]= {
    val data = ListBuffer[Array[Byte]]()
    val connector = Consumer.create(EmbeddedZkKafkaCluster.createConsumerConfig(zkConnectString, groupId))
    connectors += connector

    val streams = connector
      .createMessageStreams(Map(topic -> 1), new StringDecoder(), new DefaultDecoder())
      .get(topic)

    val executor = Executors.newFixedThreadPool(2)

    // consume the messages in the threads
    for (stream <- streams) {
      executor.submit(new Runnable() {
        def run() {
          for (s <- stream) {
            while (s.iterator.hasNext) {
              data += s.iterator.next.message
            }
          }
        }
      })
    }

    data
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

  def createConsumerConfig(zkConnect: String, groupId: String): ConsumerConfig = {
    val props = new Properties()
    props.put("zookeeper.connect", zkConnect)
    props.put("group.id", groupId)
    props.put("zookeeper.session.timeout.ms", "400")
    props.put("zookeeper.sync.time.ms", "200")
    props.put("auto.commit.interval.ms", "1000")
    return new ConsumerConfig(props)
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
