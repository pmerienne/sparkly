package sparkly.core

import java.util.Date

import org.scalameter.api._
import org.scalatest._

import scala.util.Random

class FeaturesSpec extends FlatSpec with Matchers {

  "Feature" should "be optional" in {
    Feature(None).isEmpty should be (true)
    Feature(None).isDefined should be (false)

    Feature("42").isEmpty should be (false)
    Feature("42").isDefined should be (true)

    Feature(None).getOrElse(1.5) should be (1.5)
  }


  "Feature List" should "be converted" in {
    val list = FeatureList(List( Feature("42"), Feature(43.43), Feature(44), Feature(45L), Feature(true)))

    list.asDoubleList should contain inOrderOnly (42.0, 43.43, 44.0, 45.0, 1.0)
    list.asDoubleArray should contain inOrderOnly (42.0, 43.43, 44.0, 45.0, 1.0)
  }

  "String feature" should "should be converted to other types" in {
    Feature("42").asString should be ("42")
    Feature("42.42").asDouble should be (42.42)
    Feature("42").asInt should be (42)
    Feature("42").asLong should be (42L)
    Feature("1610-07-10T00:00:00.000+00:00").asDate.getTime should be (-11344060800000L)
    Feature("true").asBoolean should be (true)
  }

  "Double feature" should "should be converted to other types" in {
    Feature(42.42).asString should be ("42.42")
    Feature(42.42).asDouble should be (42.42)
    Feature(42.42).asInt should be (42)
    Feature(42.42).asLong should be (42L)
    intercept[IllegalArgumentException] {Feature(42.42).asDate}
    Feature(42.42).asBoolean should be (true)
    Feature(42.42).asDoubleArray should be (Array(42.42))
  }

  "Int feature" should "should be converted to other types" in {
    Feature(42).asString should be ("42")
    Feature(42).asDouble should be (42.0)
    Feature(42).asInt should be (42)
    Feature(42).asLong should be (42L)
    Feature(0).asDate should be (new Date(0))
    Feature(42).asBoolean should be (true)
    Feature(42).asDoubleArray should be (Array(42.0))
  }

  "Long feature" should "should be converted to other types" in {
    val now = new Date()

    Feature(42L).asString should be ("42")
    Feature(42L).asDouble should be (42.0)
    Feature(42L).asInt should be (42)
    Feature(42L).asLong should be (42L)
    Feature(now.getTime).asDate should be (now)
    Feature(42L).asBoolean should be (true)
    Feature(42L).asDoubleArray should be (Array(42.0))
  }

  "Date feature" should "should be converted to other types" in {
    val beginningOfTime = new Date(0)

    Feature(beginningOfTime).asString should be ("1970-01-01T00:00:00.000Z")
    intercept[IllegalArgumentException] {Feature(beginningOfTime).asDouble}
    Feature(beginningOfTime).asInt should be (0)
    Feature(beginningOfTime).asLong should be (0L)
    Feature(beginningOfTime).asDate should be (beginningOfTime)
    intercept[IllegalArgumentException] {Feature(beginningOfTime).asBoolean}
  }

  "Boolean feature" should "should be converted to other types" in {
    Feature(true).asString should be ("true")
    Feature(true).asDouble should be (1.0)
    Feature(true).asInt should be (1)
    Feature(true).asLong should be (1L)
    Feature(true).asBoolean should be (true)
    intercept[IllegalArgumentException] {Feature(true).asDate}
  }

  "Vector feature" should "should not be converted to other types" in {
    intercept[IllegalArgumentException] {Feature(Array.fill(4)(0.0)).asString}
    intercept[IllegalArgumentException] {Feature(Array.fill(4)(0.0)).asDouble}
    intercept[IllegalArgumentException] {Feature(Array.fill(4)(0.0)).asInt}
    intercept[IllegalArgumentException] {Feature(Array.fill(4)(0.0)).asLong}
    intercept[IllegalArgumentException] {Feature(Array.fill(4)(0.0)).asBoolean}
    intercept[IllegalArgumentException] {Feature(Array.fill(4)(0.0)).asDate}
  }
}


object FeatureBench extends PerformanceTest.Quickbenchmark {

  val size = 10000

  val doubleGen = Gen.single("data")(FeatureList((1 to size).map(i => Feature(Random.nextDouble)).toList))
  val doubleArrayGen = Gen.single("data"){
    val arr = (1 to size).map(i => Random.nextDouble).toArray
    val feature = Feature(arr)
    FeatureList(List(feature))
  }

  val mixedGen = Gen.single("data"){
    var features = List[Feature[_]]()
    var i = 0
    while(i < size) {
      if(Random.nextDouble > 0.5) {
        features = Feature(Random.nextDouble) :: features
        i = i + 1
      } else {
        val arrSize = Math.min(size - i, Random.nextInt(100))
        features = Feature((1 to arrSize).map(i => Random.nextDouble).toArray) :: features
        i = i + arrSize
      }
    }
    FeatureList(features)
  }


  performance of "FeatureList" in {
    // measurements: 0.135051, 0.13616, 0.136121, 0.135437, 0.135923, 0.15016, 0.152859, 0.13814, 0.137214, 0.147181, 0.135291, 0.134799, 0.13554, 0.134596, 0.135318, 0.135967, 0.148925, 0.135673, 0.137387, 0.157736, 0.138549, 0.135546, 0.136572, 0.137023, 0.136337, 0.157782, 0.150274, 0.145428, 0.1604, 0.143355, 0.143795, 0.155372, 0.144867, 0.172326, 0.146526, 0.195719
    measure method "asDoubles" in {
      using(doubleGen) in { data =>
        data.asDoubleList
      }
    }

    // measurements: 0.180018, 0.163212, 0.159869, 0.179342, 0.176651, 0.169216, 0.162564, 0.165148, 0.167879, 0.16181, 0.165431, 0.171603, 0.166883, 0.162235, 0.164847, 0.163242, 0.170135, 0.167671, 0.170806, 0.175881, 0.158467, 0.156768, 0.158827, 0.168882, 0.207305, 0.169775, 0.167177, 0.164204, 0.167666, 0.164733, 0.162241, 0.163021, 0.160916, 0.165994, 0.171687, 0.213967
    measure method "asDoubleArray with 1 array" in {
      using(doubleArrayGen) in { data =>
        data.asDoubleArray
      }
    }

    // measurements: 0.281203, 0.254219, 0.254895, 0.252279, 0.262952, 0.255585, 0.255275, 0.257967, 0.255981, 0.283049, 0.258184, 0.255602, 0.262669, 0.256532, 0.256697, 0.260852, 0.313222, 0.301689, 0.334452, 0.292956, 0.30771, 0.29562, 0.290447, 0.315156, 0.29036, 0.292705, 0.906557, 1.168666, 1.147214, 1.158205, 2.137038, 5.561589, 1.883055, 1.837243, 1.772955, 2.033378
    measure method "asDoubleArray with mixed features" in {
      using(mixedGen) in { data =>
        data.asDoubleArray
      }
    }

    // measurements: 0.190866, 0.193778, 0.190081, 0.208937, 0.193554, 0.192043, 0.193214, 0.188423, 0.206004, 0.191808, 0.193915, 0.192865, 0.192399, 0.191791, 0.193326, 0.191706, 0.195071, 0.195566, 0.200145, 0.201118, 0.192409, 0.192498, 0.214597, 0.205907, 0.20096, 0.20659, 0.190029, 0.192126, 0.190401, 0.214752, 0.195165, 0.239194, 0.196498, 0.200571, 0.203092, 0.234581
    measure method "asDoubleArray with double features" in {
      using(doubleGen) in { data =>
        data.asDoubleArray
      }
    }
  }
}
