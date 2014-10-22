package pythia.visualization

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import pythia.core.{Instance, VisualizationClient}

abstract class Visualization {

  def init(stream: DStream[Instance], ssc: StreamingContext, client: VisualizationClient)

}
