package sparkly.component.enrichment

import sparkly.testing._
import sparkly.core._
import com.github.nscala_time.time.Imports._
import org.apache.cassandra.utils.UUIDGen
import java.util.UUID

class UuidEnrichmentSpec extends ComponentSpec {

  "Uuid Enrichments" should "enrich uuid from stream with date" in {
    // Given
    val configuration = ComponentConfiguration (
      name = "Add UUID",
      clazz = classOf[UuidEnrichment].getName,
      inputs = Map("In" -> StreamConfiguration(mappedFeatures = Map("Date" -> "date"))),
      outputs = Map("Out" -> StreamConfiguration(mappedFeatures = Map("UUID" -> "uuid")))
    )

    // When
    val component = deployComponent(configuration)
    component.inputs("In").push(
      Instance("date" -> "2015-02-04T00:00:00.000Z"),
      Instance("date" -> DateTime.parse("2015-02-05T00:00:00.000Z").toDate),
      Instance("date" -> null)
    )

    // Then
    eventually {
      val uuid1 = component.outputs("Out").features.filter(data => data("date") == "2015-02-04T00:00:00.000Z").head("uuid").asInstanceOf[String]
      UUIDGen.getAdjustedTimestamp(UUID.fromString(uuid1)) should equal (DateTime.parse("2015-02-04T00:00:00.000Z").getMillis)

      val uuid2 = component.outputs("Out").features.filter(data => data("date") == DateTime.parse("2015-02-05T00:00:00.000Z").toDate).head("uuid").asInstanceOf[String]
      UUIDGen.getAdjustedTimestamp(UUID.fromString(uuid2)) should equal (DateTime.parse("2015-02-05T00:00:00.000Z").getMillis)

      val uuid3 = component.outputs("Out").features.filter(data => data("date") == null).head("uuid").asInstanceOf[String]
      UUIDGen.getAdjustedTimestamp(UUID.fromString(uuid3)) should equal (System.currentTimeMillis +- 60 * 1000)
    }
  }


  "Uuid Enrichments" should "enrich stream without date" in {
    // Given
    val configuration = ComponentConfiguration (
      name = "Add UUID",
      clazz = classOf[UuidEnrichment].getName,
      inputs = Map("In" -> StreamConfiguration(mappedFeatures = Map())),
      outputs = Map("Out" -> StreamConfiguration(mappedFeatures = Map("UUID" -> "uuid")))
    )

    // When
    val component = deployComponent(configuration)
    component.inputs("In").push(Instance(), Instance(), Instance())

    // Then
    eventually {
      val uuids = component.outputs("Out").features.map(data => data("uuid").asInstanceOf[String])
      uuids.foreach{ uuid =>
        UUIDGen.getAdjustedTimestamp(UUID.fromString(uuid)) should equal (System.currentTimeMillis +- 60 * 1000)
      }

      uuids.toSet should have size (3)
    }
  }
}
