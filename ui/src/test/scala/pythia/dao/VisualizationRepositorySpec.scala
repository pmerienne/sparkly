package pythia.dao

import org.scalatest._
import pythia.testing._

class VisualizationRepositorySpec extends FlatSpec with Matchers {

  val visualizationBasePackage = "pythia.testing"

  "Component repository" should "load component's metadata" in {
    val repository = new VisualizationRepository(visualizationBasePackage)

    repository.visualizations() should contain only (
      "pythia.testing.TestVisualization" -> new TestVisualization().metadata
    )
  }

  "Component repository" should "load component metadata" in {
    val repository = new VisualizationRepository(visualizationBasePackage)
    repository.findByClassName("pythia.testing.TestVisualization") should equal(new TestVisualization().metadata)
  }
}
