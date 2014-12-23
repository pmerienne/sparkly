package pythia.component.misc

import pythia.core._
import pythia.component.ComponentSpec
import pythia.core.StreamConfiguration
import pythia.core.ComponentConfiguration
import pythia.testing.InspectedStream

class BasicFilterSpec extends ComponentSpec {

  "Basic filter" should "filter null value" in {
    // Given
    val inputStream = mockedStream()
    val configuration = ComponentConfiguration (
      clazz = classOf[BasicFilter].getName,
      name = "BasicFilter",
      inputs = Map (
        "In" -> StreamConfiguration(mappedFeatures = Map("Feature" -> "age"))
      ),
      outputs = Map (
        "Out" -> StreamConfiguration()
      ),
      properties = Map ("Operator" -> "IS NULL")
    )

    // When
    val outputs: Map[String, InspectedStream] = deployComponent(configuration, Map("In" -> inputStream.dstream))
    inputStream.push (
      Instance("name" -> "Juju", "age" -> null),
      Instance("name" -> "Pierre", "age" -> 27),
      Instance("name" -> "Fabien")
    )

    // Then
    eventually {
      outputs("Out").features should contain only (
        Map("name" -> "Juju", "age" -> null),
        Map("name" -> "Fabien")
        )
    }
  }

  "Basic filter" should "filter out null value" in {
    // Given
    val inputStream = mockedStream()
    val configuration = ComponentConfiguration (
      clazz = classOf[BasicFilter].getName,
      name = "BasicFilter",
      inputs = Map (
        "In" -> StreamConfiguration(mappedFeatures = Map("Feature" -> "age"))
      ),
      outputs = Map (
        "Out" -> StreamConfiguration()
      ),
      properties = Map ("Operator" -> "IS NOT NULL")
    )

    // When
    val outputs: Map[String, InspectedStream] = deployComponent(configuration, Map("In" -> inputStream.dstream))
    inputStream.push (
      Instance("name" -> "Juju", "age" -> null),
      Instance("name" -> "Pierre", "age" -> 27),
      Instance("name" -> "Fabien")
    )

    // Then
    eventually {
      outputs("Out").features should contain only (
        Map("name" -> "Pierre", "age" -> 27)
      )
    }
  }

  "Basic filter" should "filter value based on operand" in {
    // Given
    val inputStream = mockedStream()
    val configuration = ComponentConfiguration (
      clazz = classOf[BasicFilter].getName,
      name = "BasicFilter",
      inputs = Map (
        "In" -> StreamConfiguration(mappedFeatures = Map("Feature" -> "age"))
      ),
      outputs = Map (
        "Out" -> StreamConfiguration()
      ),
      properties = Map ("Operator" -> ">=", "Operand" -> "33")
    )

    // When
    val outputs: Map[String, InspectedStream] = deployComponent(configuration, Map("In" -> inputStream.dstream))
    inputStream.push (
      Instance("name" -> "Juju", "age" -> 33),
      Instance("name" -> "Pierre", "age" -> 27),
      Instance("name" -> "Fabien", "age" -> 29)
    )

    // Then
    eventually {
      outputs("Out").features should contain only (
        Map("name" -> "Juju", "age" -> 33)
      )
    }
  }
}
