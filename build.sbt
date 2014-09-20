import scoverage.ScoverageSbtPlugin

name := "sparkly-pythia"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  // Spark
  "org.apache.spark" %% "spark-core" % "1.0.0",
  "org.apache.spark" %% "spark-streaming" % "1.0.0",
  // Breeze for linear algebra
  "org.scalanlp" %% "breeze" % "0.8.1",
  "org.scalanlp" %% "breeze-natives" % "0.8.1",
  // Scalatra
  "org.scalatra" %% "scalatra" % "2.3.0",
  "org.eclipse.jetty" % "jetty-server" % "8.1.8.v20121106",
  "org.eclipse.jetty" % "jetty-webapp" % "8.1.8.v20121106",
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

// Packaging

packAutoSettings

packMain := Map("boot" -> "Boot")

packResourceDir += (baseDirectory.value / "src/main/webapp" -> "web-content")

packExtraClasspath := Map("boot" -> Seq("${PROG_HOME}/conf"))

packJvmOpts := Map("boot" -> Seq("-Dpythia.home=${PROG_HOME}"))

// Test coverage

ScoverageSbtPlugin.instrumentSettings
