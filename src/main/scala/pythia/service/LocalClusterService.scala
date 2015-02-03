package pythia.service

import java.util.Date

import org.apache.spark.streaming._
import pythia.config.PythiaConfig._
import pythia.core._
import pythia.dao.PipelineRepository

class LocalClusterService(
  implicit val pipelineValidationService: PipelineValidationService,
  implicit val pipelineRepository: PipelineRepository) {

  val streamingContextFactory = new StreamingContextFactory(BASE_CHECKPOINTS_DIRECTORY, "local[16]", "local", Seconds(1), HOSTNAME, WEB_PORT)
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

    streamingContext.foreach{ ssc =>
      try {
        ssc.stop(stopSparkContext = true, stopGracefully = stopGracefully)
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
      streamingContext = Some(streamingContextFactory.createStreamingContext(pipeline)._1)
      streamingContext.get.start()

      status = ClusterStatus(ClusterState.Running, Some(new Date()), Some(pipeline))
    } catch {
      case e: Exception => {
        stop(false)
        // TODO : should log!!
        throw new IllegalArgumentException(s"Unable to start pipeline ${pipeline.id}", e)
      }
    }
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
