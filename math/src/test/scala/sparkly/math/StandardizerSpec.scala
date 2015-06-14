package sparkly.math

import org.scalatest._
import scala.util.Random
import breeze.linalg.DenseVector
import org.scalameter.api._

class StandardizerSpec extends FlatSpec with Matchers {

  "Standardizer" should "standardize vectors" in {
    // Given
    val size = 1000
    val features = 5

    val originalValues = (1 to size).map{i =>
      val data = (1 to features).map(j => j * Random.nextGaussian + (j / 2)).toArray
      DenseVector(data)
    }


    // When
    val standardizer = Standardizer(features, originalValues.iterator)

    val standardizedValues = originalValues.map{ vector =>
      standardizer.standardize(vector)
    }

    // Then
    (0 until features).foreach{ i =>
      val values = standardizedValues.map(_(i)).toList
      (values.sum / values.size) should be (0.0 +- 0.1)
      mean(values) should be (0.0 +- 0.1)
      stddev(values) should be (1.0 +- 0.1)
    }
  }

  "Standardizer" should "support merge" in {
    // Given
    val size = 1000
    val features = 5

    val (originalValues1, originalValues2) = (1 to size).map{i =>
      val data = (1 to features).map(j => j * Random.nextGaussian + (j / 2)).toArray
      DenseVector(data)
    }.splitAt(size / 2)

    val standardizer1 = Standardizer(features, originalValues1.iterator)
    val standardizer2 = Standardizer(features, originalValues2.iterator)

    // When
    val standardizer = standardizer1 + standardizer2
    val standardizedValues = originalValues1.map(vector => standardizer.standardize(vector)) ++ originalValues2.map(vector => standardizer.standardize(vector))

    // Then
    (0 until features).foreach{ i =>
      val values = standardizedValues.map(_(i)).toList
      (values.sum / values.size) should be (0.0 +- 0.1)
      mean(values) should be (0.0 +- 0.1)
      stddev(values) should be (1.0 +- 0.1)
    }
  }


  def stddev(values: Iterable[Double]): Double = {
    val mean = values.sum / values.size
    val devs = values.map(v => (v - mean) * (v - mean))
    Math.sqrt(devs.sum / values.size)
  }

  def mean(values: Iterable[Double]): Double = values.sum / values.size
}

object StandardizerBench extends PerformanceTest.Quickbenchmark {

  val features = 10
  val size = 10000
  val partitions = 10

  val dataGen = Gen.single("data")(List.fill(size)(DenseVector.rand[Double](features)))
  val standardizerGen = Gen.single("standarizer")(List.fill(partitions){
    val data = List.fill(size)(DenseVector.rand[Double](features))
    Standardizer(features, data.iterator)
  })

  performance of "FeatureReservoirSampling" in {
    // measurements: 0.387005, 0.339995, 0.435124, 0.341314, 0.340185, 0.342308, 0.364161, 0.379414, 0.343344, 0.340182, 0.340852, 0.34115, 0.371088, 0.371049, 0.414009, 0.428819, 0.342322, 0.420277, 0.345605, 0.465765, 0.39953, 0.443903, 0.343658, 0.433997, 0.487961, 0.764875, 0.81152, 0.783078, 0.873337, 0.8011, 0.765952, 0.554938, 0.524392, 0.592608, 0.592292, 1.116209
    measure method "create" in {
      using(dataGen) in { data =>
        Standardizer(features, data.iterator)
      }
    }

    // measurements: 0.057963, 0.057834, 0.057804, 0.057735, 0.057923, 0.05795, 0.062904, 0.057818, 0.05775, 0.057819, 0.057821, 0.058073, 0.057717, 0.057625, 0.057807, 0.057803, 0.057706, 0.057881, 0.058123, 0.058244, 0.058419, 0.057966, 0.058103, 0.05797, 0.058209, 0.058227, 0.057909, 0.057766, 0.058315, 0.140962, 0.078868, 0.078579, 0.079885, 0.090037, 0.08056, 0.14698
    measure method "merge" in {
      using(standardizerGen) in { standardizers =>
        standardizers.reduce(_ + _)
      }
    }
  }
}