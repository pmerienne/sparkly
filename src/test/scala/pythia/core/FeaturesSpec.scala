package pythia.core

import java.util.Date

import org.scalatest._

class FeaturesSpec extends FlatSpec with Matchers {

  "Feature" should "be optional" in {
    Feature(None).isEmpty should be (true)
    Feature(None).isDefined should be (false)

    Feature(Some("42")).isEmpty should be (false)
    Feature(Some("42")).isDefined should be (true)

    Feature(None).as[Double] should be (null.asInstanceOf[Double])
  }

  "Feature List" should "be converted" in {
    val list = FeatureList(List( Feature(Some("42")),  Feature(Some(43.43)), Feature(Some(44)), Feature(Some(45L)), Feature(Some(true))))

    list.as[Double] should contain only (42.0, 43.43, 44.0, 45.0, 1.0)
    list.asArrayOf[Double] should contain only (42.0, 43.43, 44.0, 45.0, 1.0)
  }

  "String feature" should "should be converted to other types" in {
    Feature(Some("42")).as[String] should be ("42")
    Feature(Some("42.42")).as[Double] should be (42.42)
    Feature(Some("42")).as[Int] should be (42)
    Feature(Some("42")).as[Long] should be (42L)
    Feature(Some("1610-07-10T00:00:00.000+00:00")).as[Date].getTime should be (-11344060800000L)
    Feature(Some("true")).as[Boolean] should be (true)
  }

  "Double feature" should "should be converted to other types" in {
    Feature(Some(42.42)).as[String] should be ("42.42")
    Feature(Some(42.42)).as[Double] should be (42.42)
    Feature(Some(42.42)).as[Int] should be (42)
    Feature(Some(42.42)).as[Long] should be (42L)
    intercept[IllegalArgumentException] {Feature(Some(42.42)).as[Date]}
    Feature(Some(42.42)).as[Boolean] should be (true)
  }

  "Int feature" should "should be converted to other types" in {
    Feature(Some(42)).as[String] should be ("42")
    Feature(Some(42)).as[Double] should be (42.0)
    Feature(Some(42)).as[Int] should be (42)
    Feature(Some(42)).as[Long] should be (42L)
    Feature(Some(0)).as[Date] should be (new Date(0))
    Feature(Some(42)).as[Boolean] should be (true)
  }

  "Long feature" should "should be converted to other types" in {
    val now = new Date()

    Feature(Some(42L)).as[String] should be ("42")
    Feature(Some(42L)).as[Double] should be (42.0)
    Feature(Some(42L)).as[Int] should be (42)
    Feature(Some(42L)).as[Long] should be (42L)
    Feature(Some(now.getTime)).as[Date] should be (now)
    Feature(Some(42L)).as[Boolean] should be (true)
  }

  "Date feature" should "should be converted to other types" in {
    val beginningOfTime = new Date(0)

    Feature(Some(beginningOfTime)).as[String] should be ("1970-01-01T00:00:00.000Z")
    intercept[IllegalArgumentException] {Feature(Some(beginningOfTime)).as[Double]}
    Feature(Some(beginningOfTime)).as[Int] should be (0)
    Feature(Some(beginningOfTime)).as[Long] should be (0L)
    Feature(Some(beginningOfTime)).as[Date] should be (beginningOfTime)
    intercept[IllegalArgumentException] {Feature(Some(beginningOfTime)).as[Boolean]}
  }

  "Boolean feature" should "should be converted to other types" in {
    val now = new Date()

    Feature(Some(true)).as[String] should be ("true")
    Feature(Some(true)).as[Double] should be (1.0)
    Feature(Some(true)).as[Int] should be (1)
    Feature(Some(true)).as[Long] should be (1L)
    intercept[IllegalArgumentException] {Feature(Some(true)).as[Date]}
    Feature(Some(true)).as[Boolean] should be (true)
  }
}
