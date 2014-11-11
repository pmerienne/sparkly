package pythia.dao

import org.scalatest._
import pythia.testing.component.{TestClassifier, TestComponent}

class ComponentRepositorySpec extends FlatSpec with Matchers {

  val componentBasePackage = "pythia.testing.component"

  val testComponentMetadata = new TestComponent().metadata
  val testClassifierMetadata = new TestClassifier().metadata

  "Component repository" should "load component's metadata" in {
    val repository = new ComponentRepository(componentBasePackage)

    repository.components() should contain only (
      "pythia.testing.component.TestClassifier" -> testClassifierMetadata,
      "pythia.testing.component.TestComponent" -> testComponentMetadata
    )
  }

  "Component repository" should "load component metadata" in {
    val repository = new ComponentRepository(componentBasePackage)
    repository.component("pythia.testing.component.TestClassifier").get should equal(testClassifierMetadata)
  }
}
