package sparkly.dao

import org.scalatest._

import sparkly.dao._
import sparkly.testing._

class VisualizationRepositorySpec extends FlatSpec with Matchers {

  val visualizationBasePackage = "sparkly.testing"

  "Component repository" should "load component's metadata" in {
    val repository = new VisualizationRepository(visualizationBasePackage)

    repository.visualizations() should contain only (
      "sparkly.testing.TestVisualization" -> new TestVisualization().metadata
    )
  }

  "Component repository" should "load component metadata" in {
    val repository = new VisualizationRepository(visualizationBasePackage)
    repository.findByClassName("sparkly.testing.TestVisualization") should equal(new TestVisualization().metadata)
  }
}
