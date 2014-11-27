package pythia.service

import java.util.Date

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import pythia.config.PythiaConfig._
import pythia.core.{PipelineBuilder, PipelineConfiguration, VisualizationBuilder}
import pythia.dao.PipelineRepository

class LocalClusterService(
  implicit val pipelineBuilder: PipelineBuilder,
  implicit val visualizationBuilder: VisualizationBuilder,
  implicit val pipelineValidationService: PipelineValidationService,
  implicit val pipelineRepository: PipelineRepository) {

  val sparckConfig = new SparkConf()

  val sparkContext = new SparkContext("local[4]", "pythia", sparckConfig)
  var streamingContext: Option[StreamingContext] = None

  var status: ClusterStatus = ClusterStatus(ClusterState.Stopped, None, None)

  def deploy(pipelineId: String, stopGracefully: Boolean = true) {
    pipelineRepository.get(pipelineId) match {
      case None => throw new IllegalArgumentException("Pipeline with id " + pipelineId + " does not exists")
      case Some(pipeline) => {
        check(pipeline)
        stop(stopGracefully)
        start(pipeline)
      }
    }
  }

  def check(pipeline: PipelineConfiguration): Unit = {
    val report = pipelineValidationService.validate(pipeline)
    if(report.containsErrors) {
      throw new IllegalArgumentException(s"Pipeline with id ${pipeline.id} is not valid. Validation report : \n${report}")
    }
  }

  def stop(stopGracefully: Boolean) {
    status = ClusterStatus(ClusterState.Stopping, None, None)

    if(streamingContext.isDefined) {
      val ssc = streamingContext.get
      try {
        ssc.stop(stopSparkContext = false, stopGracefully = stopGracefully)
        ssc.awaitTermination(5000)
        streamingContext = None
      } catch {
        case e: Exception => println("Unable to stop streaming context")  // TODO : should log!!
      }
    }

    status = ClusterStatus(ClusterState.Stopped, None, None)
  }

  def start(pipeline: PipelineConfiguration) {
    status = ClusterStatus(ClusterState.Deploying, Some(new Date()), Some(pipeline))

    try {
      val context = initStreamingContext()
      val outputStreams = pipelineBuilder.build(context, pipeline)
      visualizationBuilder.buildVisualizations("local", context, pipeline, outputStreams)
      context.start()

      status = ClusterStatus(ClusterState.Running, Some(new Date()), Some(pipeline))
    } catch {
      case e: Exception => {
        stop(false)
        // TODO : should log!!
        throw new IllegalArgumentException(s"Unable to start pipeline ${pipeline.id}", e)
      }
    }
  }

  private def initStreamingContext(): StreamingContext = {
    val ssc = new StreamingContext(sparkContext, Seconds(1))
    ssc.checkpoint(CHECKPOINTS_DIRECTORY)

    streamingContext = Some(ssc)
    ssc
  }

}

case class ClusterStatus(action: ClusterState.ClusterAction, time: Option[Date], pipeline: Option[PipelineConfiguration]) {
  def isReadyToDeploy() = action match {
    case ClusterState.Stopped => true
    case ClusterState.Running => true
    case _ => false
  }
}

object ClusterState extends Enumeration {
  type ClusterAction = Value
  val Stopped, Deploying, Running, Stopping = Value
}