package pythia.testing.visualization

import pythia.core.{VisualizationContext, VisualizationMetadata, Visualization}

class TestVisualization extends Visualization {
  def metadata = VisualizationMetadata("Test")
  override def init(context: VisualizationContext): Unit = ???
}
