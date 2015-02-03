package pythia.core

import org.apache.spark.SparkConf
import org.apache.spark.streaming._

import scala.reflect.io.Path

class StreamingContextFactory (
  val checkpointBaseDirectory: String,
  val master: String,
  val clusterId: String,
  val batchDuration: Duration,
  val sparklyHostname: String, val sparklyPort: Int) {

  val pipelineBuilder = new PipelineBuilder()
  val visualizationBuilder = new VisualizationBuilder(sparklyHostname, sparklyPort)

  def restoreStreamingContext(pipeline: PipelineConfiguration): StreamingContext = {
    val checkpointDirectory = getCheckpointDirectory(pipeline).toString
    StreamingContext.getOrCreate(checkpointDirectory, build(pipeline, checkpointDirectory)._1 _, createOnError = false)
  }

  def createStreamingContext(pipeline: PipelineConfiguration): (StreamingContext, BuildResult) = {
    val checkpointDirectory =  getCheckpointDirectory(pipeline)
    checkpointDirectory.deleteRecursively()
    build(pipeline, checkpointDirectory.toString)
  }

  private def build(pipeline: PipelineConfiguration, checkpointDirectory: String): (StreamingContext, BuildResult) = {
    val conf = createSparkConf()

    val ssc = new StreamingContext(conf, batchDuration)
    ssc.checkpoint(checkpointDirectory)

    val buildResult = pipelineBuilder.build(ssc, pipeline)
    visualizationBuilder.buildVisualizations(clusterId, ssc, pipeline, buildResult.outputs)
    (ssc, buildResult)
  }

  private def createSparkConf(): SparkConf = {
    new SparkConf()
      .setAppName("sparkly")
      .setMaster(master)
      .set("spark.streaming.receiver.writeAheadLogs.enable", "true")
  }

  private def getCheckpointDirectory(pipeline: PipelineConfiguration) = Path(List(checkpointBaseDirectory, pipeline.id))
}
