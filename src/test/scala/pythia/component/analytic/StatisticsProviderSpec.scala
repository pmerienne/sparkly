package pythia.component.analytic

import pythia.component.ComponentSpec
import pythia.core.{PropertyMetadata, Instance, StreamConfiguration, ComponentConfiguration}
import pythia.testing.InspectedStream


class StatisticsProviderSpec extends ComponentSpec {

  "Statistic provider" should "compute global stat" in {
    // Given
    val inputStream = mockedStream()
    val configuration = ComponentConfiguration (
      clazz = classOf[StatisticsProvider].getName,
      name = "Statistic Provider",
      inputs = Map (
        "Input" -> StreamConfiguration(mappedFeatures = Map("Compute on" -> "View"))
      ),
      outputs = Map (
        "Output" -> StreamConfiguration(mappedFeatures = Map("Result" -> "View count"))
      ),
      properties = Map("Operation" -> "Count")
    )

    // When
    val outputs: Map[String, InspectedStream] = deployComponent(configuration, Map("Input" -> inputStream.dstream))
    inputStream.push (
      Instance("Page" -> "index.html", "User" -> "pmerienne", "View" -> 15),
      Instance("Page" -> "sells.html", "User" -> "pmerienne", "View" -> 5),
      Instance("Page" -> "about.html", "User" -> "pmerienne", "View" -> 1),
      Instance("Page" -> "index.html", "User" -> "jchanut", "View" -> 10),
      Instance("Page" -> "sells.html", "User" -> "jchanut", "View" -> 25)
    )

    // Then
    eventually {
      outputs("Output").features should contain only (
        Map("Page" -> "index.html", "User" -> "pmerienne", "View" -> 15, "View count" -> 5),
        Map("Page" -> "sells.html", "User" -> "pmerienne", "View" -> 5, "View count" -> 5),
        Map("Page" -> "about.html", "User" -> "pmerienne", "View" -> 1, "View count" -> 5),
        Map("Page" -> "index.html", "User" -> "jchanut", "View" -> 10, "View count" -> 5),
        Map("Page" -> "sells.html", "User" -> "jchanut", "View" -> 25, "View count" -> 5)
      )
    }
  }


  "Statistic provider" should "compute grouped stat" in {
    // Given
    val inputStream = mockedStream()
    val configuration = ComponentConfiguration (
      clazz = classOf[StatisticsProvider].getName,
      name = "Statistic Provider",
      inputs = Map (
        "Input" -> StreamConfiguration(mappedFeatures = Map("Compute on" -> "View", "Group by" -> "User"))
      ),
      outputs = Map (
        "Output" -> StreamConfiguration(mappedFeatures = Map("Result" -> "User's view count"))
      ),
      properties = Map("Operation" -> "Count")
    )

    // When
    val outputs: Map[String, InspectedStream] = deployComponent(configuration, Map("Input" -> inputStream.dstream))
    inputStream.push (
      Instance("Page" -> "index.html", "User" -> "pmerienne", "View" -> 15),
      Instance("Page" -> "sells.html", "User" -> "pmerienne", "View" -> 5),
      Instance("Page" -> "about.html", "User" -> "pmerienne", "View" -> 1),
      Instance("Page" -> "index.html", "User" -> "jchanut", "View" -> 10),
      Instance("Page" -> "sells.html", "User" -> "jchanut", "View" -> 25)
    )

    // Then
    eventually {
      outputs("Output").features should contain only (
        Map("Page" -> "index.html", "User" -> "pmerienne", "View" -> 15, "User's view count" -> 3),
        Map("Page" -> "sells.html", "User" -> "pmerienne", "View" -> 5, "User's view count" -> 3),
        Map("Page" -> "about.html", "User" -> "pmerienne", "View" -> 1, "User's view count" -> 3),
        Map("Page" -> "index.html", "User" -> "jchanut", "View" -> 10, "User's view count" -> 2),
        Map("Page" -> "sells.html", "User" -> "jchanut", "View" -> 25, "User's view count" -> 2)
      )
    }
  }


  "Statistic provider" should "compute global stat in time window" in {
    // Given
    val inputStream = mockedStream()
    val configuration = ComponentConfiguration (
      clazz = classOf[StatisticsProvider].getName,
      name = "Statistic Provider",
      inputs = Map (
        "Input" -> StreamConfiguration(mappedFeatures = Map("Compute on" -> "View"))
      ),
      outputs = Map (
        "Output" -> StreamConfiguration(mappedFeatures = Map("Result" -> "Last second view count"))
      ),
      properties = Map (
        "Operation" -> "Count",
        "Window length (in ms)" -> "1000"
      )
    )

    // When
    val outputs: Map[String, InspectedStream] = deployComponent(configuration, Map("Input" -> inputStream.dstream))
    inputStream.push (
      Instance("Page" -> "index.html", "User" -> "pmerienne", "View" -> 15),
      Instance("Page" -> "sells.html", "User" -> "pmerienne", "View" -> 5),
      Instance("Page" -> "about.html", "User" -> "pmerienne", "View" -> 1),
      Instance("Page" -> "index.html", "User" -> "jchanut", "View" -> 10),
      Instance("Page" -> "sells.html", "User" -> "jchanut", "View" -> 25)
    )
    Thread sleep 1500
    inputStream.push (
      Instance("Page" -> "index.html", "User" -> "pmerienne", "View" -> 15),
      Instance("Page" -> "sells.html", "User" -> "jchanut", "View" -> 25)
    )
    // Then
    eventually {
      outputs("Output").features should contain only (
        Map("Page" -> "index.html", "User" -> "pmerienne", "View" -> 15, "Last second view count" -> 5),
        Map("Page" -> "sells.html", "User" -> "pmerienne", "View" -> 5, "Last second view count" -> 5),
        Map("Page" -> "about.html", "User" -> "pmerienne", "View" -> 1, "Last second view count" -> 5),
        Map("Page" -> "index.html", "User" -> "jchanut", "View" -> 10, "Last second view count" -> 5),
        Map("Page" -> "sells.html", "User" -> "jchanut", "View" -> 25, "Last second view count" -> 5),


        Map("Page" -> "index.html", "User" -> "pmerienne", "View" -> 15, "Last second view count" -> 2),
        Map("Page" -> "sells.html", "User" -> "jchanut", "View" -> 25, "Last second view count" -> 2)
      )
    }

    Thread sleep 1000
    outputs("Output").count() should be (7)
  }
}
