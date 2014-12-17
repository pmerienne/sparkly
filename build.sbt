import scoverage.ScoverageSbtPlugin
import sbtrelease.ReleasePlugin.ReleaseKeys._

name := "sparkly-pythia"

scalaVersion := "2.10.4"

parallelExecution := false

val jettyVersion = "8.1.14.v20131031"

jetty()

resolvers += Resolver.sonatypeRepo("public")

libraryDependencies ++= Seq(
  // Spark
  "org.apache.spark" %% "spark-core" % "1.1.0",
  "org.apache.spark" %% "spark-streaming" % "1.1.0",
  "org.apache.spark" %% "spark-streaming-twitter" % "1.1.0",
  "org.apache.spark" %% "spark-mllib" % "1.1.0",
  // Breeze for linear algebra
  "org.scalanlp" %% "breeze" % "0.8.1",
  "org.scalanlp" %% "breeze-natives" % "0.8.1",
  // Math/Analytics/DM lib
  "com.twitter" %% "algebird-core" % "0.8.1",
  "org.apache.commons" % "commons-math3" % "3.2",
  "org.apache.mahout" % "mahout-math" % "0.9",
  "nz.ac.waikato.cms.moa" % "moa" % "2014.04",
  // Scalatra
  "org.scalatra" %% "scalatra" % "2.3.0",
  "org.scalatra" %% "scalatra-atmosphere" % "2.3.0" exclude("com.typesafe.akka", "akka-actor_2.10"),
  "org.eclipse.jetty" %  "jetty-plus" % jettyVersion % "compile;provided",
  "org.eclipse.jetty" % "jetty-webapp" % jettyVersion % "container;compile",
  "org.eclipse.jetty" % "jetty-websocket" % jettyVersion % "compile;provided",
  "org.scalatra" %% "scalatra-json" % "2.3.0",
  "org.json4s"   %% "json4s-jackson" % "3.2.9",
  // Web-Socket client
  "org.atmosphere" % "wasync" % "1.4.0",
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

dependencyOverrides += "org.eclipse.jetty" % "jetty-websocket" % jettyVersion

dependencyOverrides += "org.eclipse.jetty" % "jetty-io" % jettyVersion

dependencyOverrides += "org.eclipse.jetty" % "jetty-jndi" % jettyVersion

dependencyOverrides += "org.eclipse.jetty" % "jetty-http" % jettyVersion

dependencyOverrides += "org.eclipse.jetty" % "jetty-plus" % jettyVersion

dependencyOverrides += "org.eclipse.jetty" % "jetty-security" % jettyVersion

dependencyOverrides += "org.eclipse.jetty" % "jetty-util" % jettyVersion

// Packaging

packAutoSettings

packMain := Map("boot" -> "Boot")

packResourceDir += (baseDirectory.value / "src/main/webapp" -> "web-content")

packResourceDir += (baseDirectory.value / "src/main/resources" -> "conf")

packExtraClasspath := Map("boot" -> Seq("${PROG_HOME}/conf"))

packJvmOpts := Map("boot" -> Seq("-Dpythia.home=${PROG_HOME}"))

// Test coverage

ScoverageSbtPlugin.instrumentSettings

CoverallsPlugin.coverallsSettings

// Release

releaseSettings

useGlobalVersion := false

publishTo := Some(Resolver.file("file",  new File( "target/releases" )) )

// Dependency tree
net.virtualvoid.sbt.graph.Plugin.graphSettings