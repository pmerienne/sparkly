package pythia.dao

import org.scalatest._
import pythia.testing._

class ComponentRepositorySpec extends FlatSpec with Matchers {

  val componentBasePackage = "pythia.testing"

  val mockStreamMetadata = new MockStream().metadata
  val testComponentMetadata = new TestComponent().metadata
  val testClassifierMetadata = new TestClassifier().metadata

  "Component repository" should "load component's metadata" in {
    val repository = new ComponentRepository(componentBasePackage)

    repository.components() should contain only (
      "pythia.testing.MockStream" -> mockStreamMetadata,
      "pythia.testing.TestClassifier" -> testClassifierMetadata,
      "pythia.testing.TestComponent" -> testComponentMetadata
    )
  }

  "Component repository" should "load component metadata" in {
    val repository = new ComponentRepository(componentBasePackage)
    repository.component("pythia.testing.TestClassifier").get should equal(testClassifierMetadata)
  }
}
