package sparkly.component.source.dataset

import sparkly.testing._
import sparkly.core._

class SpamDatasetSpec extends ComponentSpec {

  val features: List[String] = SpamDataset.labelName :: SpamDataset.featureNames

  "SpamDataset" should "stream spam data" in {
    val configuration = ComponentConfiguration(
      clazz = classOf[SpamDataset].getName,
      name = "SpamDataset",
      outputs = Map("Instances" -> StreamConfiguration(mappedFeatures = features.map(name => (name, name)).toMap)),
      properties = Map(
        "Throughput (instance/second)" -> "5000"
      )
    )

    val component = deployComponent(configuration)

    eventually {
      component.outputs("Instances").instances.size should be (4601)
      component.outputs("Instances").instances.foreach{ instance =>
        instance.rawFeatures.keys should contain only (features: _*)
      }
    }
  }

  "SpamDataset" should "loop spam data" in {
    val configuration = ComponentConfiguration(
      clazz = classOf[SpamDataset].getName,
      name = "SpamDataset",
      outputs = Map("Instances" -> StreamConfiguration(mappedFeatures = features.map(name => (name, name)).toMap)),
      properties = Map(
        "Throughput (instance/second)" -> "10000",
        "Loop" -> "True"
      )
    )

    val component = deployComponent(configuration)

    eventually {
      component.outputs("Instances").instances.size should be > 8000
      component.outputs("Instances").instances.foreach{ instance =>
        instance.rawFeatures.keys should contain only (features: _*)
      }
    }
  }
}
