package pythia.component.source.generator

import pythia.testing._
import pythia.core._

class STAGGERGeneratorSpec extends ComponentSpec {

  "STAGGER source" should "generate random things" in {
    val configuration = ComponentConfiguration (
      clazz = classOf[STAGGERGenerator].getName,
      name = "Source",
      outputs = Map (
        "Instances" -> StreamConfiguration(mappedFeatures = Map (
          "size" -> "size",
          "color" -> "color",
          "shape" -> "shape",
          "class" -> "class"
        ))
      )
    )

    val component = deployComponent(configuration)

    eventually {
      component.outputs("Instances").instances.foreach { instance =>
        instance.outputFeature("size").as[String] should (be("small") or be("medium") or be("large"))
        instance.outputFeature("color").as[String] should (be("green") or be("blue") or be("red"))
        instance.outputFeature("shape").as[String] should (be("circle") or be("triangle") or be("square"))
        instance.outputFeature("class").as[String] should (be("true") or be("false"))
      }

      // Check it generates random instances
      component.outputs("Instances").instances.toSet.size should be > 1
    }
  }

}
