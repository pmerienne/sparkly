package sparkly.core

import org.apache.spark.SparkConf
import org.apache.spark.streaming._

import scala.reflect.io.Path
import sparkly.utils.SparklyDirectoryStructure._

class StreamingContextFactory (
  val baseDistributedDirectory: String,
  val master: String,
  val clusterId: String,
  val batchDuration: Duration,
  val sparklyHostname: String, val sparklyPort: Int) {

  val pipelineBuilder = new PipelineBuilder()
  val visualizationBuilder = new VisualizationBuilder(sparklyHostname, sparklyPort)

  def restoreStreamingContext(pipeline: PipelineConfiguration): StreamingContext = {
    val pipelineDirectory = getPipelineDirectory(baseDistributedDirectory, pipeline.id)
    val checkpointDirectory = getCheckpointDirectory(pipelineDirectory, pipeline.id).toString
    // TODO should give hadoop conf
    StreamingContext.getOrCreate(checkpointDirectory, build(pipeline, pipelineDirectory, checkpointDirectory)._1 _, createOnError = false)
  }

  def createStreamingContext(pipeline: PipelineConfiguration): (StreamingContext, BuildResult) = {
    val pipelineDirectory = getPipelineDirectory(baseDistributedDirectory, pipeline.id)
    val checkpointDirectory =  getCheckpointDirectory(pipelineDirectory, pipeline.id)
    Path(checkpointDirectory).deleteRecursively()
    build(pipeline, pipelineDirectory, checkpointDirectory.toString)
  }

  private def build(pipeline: PipelineConfiguration, pipelineDirectory: String, checkpointDirectory: String): (StreamingContext, BuildResult) = {
    val conf = createSparkConf(pipeline)

    val ssc = new StreamingContext(conf, batchDuration)
    ssc.checkpoint(checkpointDirectory)

    val buildResult = pipelineBuilder.build(ssc, pipelineDirectory, pipeline)
    visualizationBuilder.buildVisualizations(clusterId, ssc, pipeline, buildResult.outputs)
    (ssc, buildResult)
  }

  private def createSparkConf(pipeline: PipelineConfiguration): SparkConf = {
    val settings = pipeline.settings.values.foldLeft(Map[String, String]())((a: Map[String, String], b: Map[String, String]) => a ++ b )
    new SparkConf()
      .setAppName("sparkly")
      .setMaster(master)
      .setAll(settings)
  }

}
