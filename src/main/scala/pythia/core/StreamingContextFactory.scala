package pythia.core

import org.apache.spark.streaming._
import org.apache.spark.SparkConf
import scala.reflect.io.Path

class StreamingContextFactory (
  val checkpointBaseDirectory: String,
  val master: String,
  val clusterId: String,
  val batchDuration: Duration,
  val sparklyHostname: String, val sparklyPort: Int) {

  val pipelineBuilder = new PipelineBuilder()
  val visualizationBuilder = new VisualizationBuilder(sparklyHostname, sparklyPort)

  def createStreamingContext(pipeline: PipelineConfiguration): StreamingContext = {
    val checkpointDirectory =  Path(List(checkpointBaseDirectory, pipeline.id)).toString
    StreamingContext.getOrCreate(checkpointDirectory, buildStreamingContext(pipeline, checkpointDirectory) _, createOnError = false)
  }

  private def buildStreamingContext(pipeline: PipelineConfiguration, checkpointDirectory: String)(): StreamingContext = {
    val conf = createSparkConf()

    val ssc = new StreamingContext(conf, batchDuration)
    ssc.checkpoint(checkpointDirectory)

    val outputStreams = pipelineBuilder.build(ssc, pipeline)
    visualizationBuilder.buildVisualizations(clusterId, ssc, pipeline, outputStreams)
    ssc
  }

  private def createSparkConf(): SparkConf = {
    new SparkConf()
      .setAppName("sparkly")
      .setMaster(master)
      .set("spark.streaming.receiver.writeAheadLogs.enable", "true")
  }

}
