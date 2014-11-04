package pythia.config

import pythia.core._
import pythia.dao._
import pythia.service._
import pythia.web.model.ModelMapper
import pythia.visualization.Visualization
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

trait Bindings {

  implicit val componentBasePackage = "pythia"

  implicit val pipelineRepository = new PipelineRepository()
  implicit val componentRepository = new ComponentRepository()
  implicit val pipelineBuilder = new PipelineBuilder()
  implicit val visualizationBuilder = new VisualizationBuilder()

  implicit val pipelineValidationService = new PipelineValidationService()
  implicit val localClusterService = new LocalClusterService()

  implicit val modelMapper = new ModelMapper()
}
