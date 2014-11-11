package pythia.dao

import org.scalatest._
import pythia.testing.component.{TestClassifier, TestComponent}
import pythia.testing.visualization.TestVisualization

class VisualizationRepositorySpec extends FlatSpec with Matchers {

  val visualizationBasePackage = "pythia.testing.visualization"

  "Component repository" should "load component's metadata" in {
    val repository = new VisualizationRepository(visualizationBasePackage)

    repository.visualizations() should contain only (
      "pythia.testing.visualization.TestVisualization" -> new TestVisualization().metadata
    )
  }

  "Component repository" should "load component metadata" in {
    val repository = new VisualizationRepository(visualizationBasePackage)
    repository.findByClassName("pythia.testing.visualization.TestVisualization") should equal(new TestVisualization().metadata)
  }
}
