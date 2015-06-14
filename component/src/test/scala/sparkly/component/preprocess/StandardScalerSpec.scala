package sparkly.component.preprocess

import sparkly.testing.ComponentSpec
import sparkly.core.{Instance, StreamConfiguration, ComponentConfiguration}
import scala.util.Random

class StandardScalerSpec extends ComponentSpec {

  "StandardScaler" should "standardize features" in {
    // Given
    val configuration = ComponentConfiguration (
      clazz = classOf[StandardScaler].getName,
      name = "StandardScaler",
      inputs = Map (
        "Input" -> StreamConfiguration(selectedFeatures = Map("Features" -> List("f1", "f2", "f3")))
      )
    )

    // When
    val component = deployComponent(configuration)
    component.inputs("Input").push (1000, () => {
      Instance("f1" -> (Random.nextGaussian * 3.0 + 2.0), "f2" -> (Random.nextGaussian * -1.5 + 1.0), "f3" -> Random.nextGaussian)
    })

    // Then
    eventually {
      component.outputs("Output").features.size should be > 500
      val f1 = component.outputs("Output").features.map(data => data("f1").asInstanceOf[Double])
      mean(f1) should be (0.0 +- 0.1)
      stddev(f1) should be (1.0 +- 0.1)

      val f2 = component.outputs("Output").features.map(data => data("f1").asInstanceOf[Double])
      mean(f2) should be (0.0 +- 0.1)
      stddev(f2) should be (1.0 +- 0.1)

      val f3 = component.outputs("Output").features.map(data => data("f1").asInstanceOf[Double])
      mean(f3) should be (0.0 +- 0.1)
      stddev(f3) should be (1.0 +- 0.1)
    }
  }


  def stddev(values: Iterable[Double]): Double = {
    val mean = values.sum / values.size
    val devs = values.map(v => (v - mean) * (v - mean))
    Math.sqrt(devs.sum / values.size)
  }

  def mean(values: Iterable[Double]): Double = values.sum / values.size
}
