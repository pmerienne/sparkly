package sparkly.component.source.dataset

import sparkly.core.{StreamConfiguration, ComponentConfiguration}
import sparkly.testing.ComponentSpec

class OptDigitsDatasetSpec extends ComponentSpec {

  "OptDigitsDataset" should "stream optdigits data" in {
    val configuration = ComponentConfiguration(
      clazz = classOf[OptDigitsDataset].getName,
      name = "OptDigitsDataset",
      outputs = Map("Instances" -> StreamConfiguration(mappedFeatures = OptDigitsDataset.labelAndFeatures.map(name => (name, name)).toMap)),
      properties = Map(
        "Throughput (instance/second)" -> "5000"
      )
    )

    val component = deployComponent(configuration)

    eventually {
      component.outputs("Instances").instances.size should be (5620)
      component.outputs("Instances").instances.foreach{ instance =>
        instance.rawFeatures.keys should contain only (OptDigitsDataset.labelAndFeatures: _*)
      }
    }
  }

}
