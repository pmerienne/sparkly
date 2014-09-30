package pythia.dao

import org.scalatest._
import pythia.core._

class ComponentRepositorySpec extends FlatSpec with Matchers {

  implicit val componentBasePackage = "pythia.dao.component"

  "Component repository" should "load component's metadata" in {
    val repository = new ComponentRepository()

    repository.components() should contain only (
      "pythia.dao.component.TestClassifier" -> ComponentMetadata("Test classifier", "Only for test purpose"),
      "pythia.dao.component.TestComponent" -> ComponentMetadata("Test component", "Only for test purpose")
    )
  }

  "Component repository" should "load component metadata" in {
    val repository = new ComponentRepository()
    repository.component("pythia.dao.component.TestClassifier").get should equal(ComponentMetadata("Test classifier", "Only for test purpose"))
  }
}
