package pythia.core

import java.util.Date

import org.scalatest._

class PropertySpec extends FlatSpec with Matchers {

  "Property" should "have default value" in {
    Property("DECIMAL", defaultValue = Some(0.25)).as[Double] should be(0.25)
    Property("DECIMAL", defaultValue = Some(0.25), selectedValue = Some("0.42")).as[Double] should be(0.42)
  }

  "Property" should "be converted to simple type" in {
    Property("STRING", selectedValue = Some("bar")).as[String] should be ("bar")
    Property("INTEGER",selectedValue = Some("42")).as[Int] should be (42)
    Property("DECIMAL", selectedValue = Some("0.25")).as[Double] should be (0.25)
    Property("DATE", selectedValue = Some("1610-07-10T00:00:00.000+00:00")).as[Date].getTime should be (-11344060800000L)
    Property("BOOLEAN", selectedValue = Some("true")).as[Boolean] should be (true)
  }

  "Property" should "have optional value" in {
    Property("STRING", selectedValue = Some("42")).isDefined should be (true)
    Property("STRING", selectedValue = Some("42")).isEmpty should be (false)

    Property("STRING").isDefined should be (false)
    Property("STRING").isEmpty should be (true)
  }
}
