import scoverage.ScoverageSbtPlugin

name := "sparkly-pythia"

version := "1.0"

scalaVersion := "2.10.4"

parallelExecution := false

// 9.2.1.v20140609
val jettyVersion = "8.1.14.v20131031"

jetty()

libraryDependencies ++= Seq(
  // Spark
  "org.apache.spark" %% "spark-core" % "1.1.0",
  "org.apache.spark" %% "spark-streaming" % "1.1.0",
  // Breeze for linear algebra
  "org.scalanlp" %% "breeze" % "0.8.1",
  "org.scalanlp" %% "breeze-natives" % "0.8.1",
  // Scalatra
  "org.scalatra" %% "scalatra" % "2.3.0",
  "org.eclipse.jetty" % "jetty-webapp" % jettyVersion % "container;compile",
  "org.scalatra" %% "scalatra-json" % "2.3.0",
  "org.json4s"   %% "json4s-jackson" % "3.2.9",
  // DB
  "org.mapdb" % "mapdb" % "1.0.6",
  // Utils
  "org.reflections" % "reflections" % "0.9.9-RC1",
  // Test
  "org.scalatest" % "scalatest_2.10" % "2.2.0" % "test",
  "org.scalatra" %% "scalatra-scalatest" % "2.3.0" % "test"
)

dependencyOverrides += "org.eclipse.jetty" % "jetty-webapp" % jettyVersion

dependencyOverrides += "org.eclipse.jetty" % "jetty-server" % jettyVersion

dependencyOverrides += "org.eclipse.jetty" % "jetty-io" % jettyVersion

dependencyOverrides += "org.eclipse.jetty" % "jetty-jndi" % jettyVersion

dependencyOverrides += "org.eclipse.jetty" % "jetty-http" % jettyVersion

dependencyOverrides += "org.eclipse.jetty" % "jetty-plus" % jettyVersion

dependencyOverrides += "org.eclipse.jetty" % "jetty-security" % jettyVersion

dependencyOverrides += "org.eclipse.jetty" % "jetty-util" % jettyVersion
// 9.2.1.v20140609

// Packaging

packAutoSettings

packMain := Map("boot" -> "Boot")

packResourceDir += (baseDirectory.value / "src/main/webapp" -> "web-content")

packExtraClasspath := Map("boot" -> Seq("${PROG_HOME}/conf"))

packJvmOpts := Map("boot" -> Seq("-Dpythia.home=${PROG_HOME}"))

// Test coverage

ScoverageSbtPlugin.instrumentSettings
