logLevel := Level.Warn

resolvers += Resolver.url(
  "bintray-sbt-plugin-releases",
   url("https://dl.bintray.com/content/sbt/sbt-plugin-releases"))(
       Resolver.ivyStylePatterns)


addSbtPlugin("com.earldouglas" % "xsbt-web-plugin" % "1.0.0-M7")

addSbtPlugin("org.xerial.sbt" % "sbt-pack"  % "0.6.2")

addSbtPlugin("org.scoverage" %% "sbt-scoverage" % "0.99.7.1")

addSbtPlugin("com.sksamuel.scoverage" %% "sbt-coveralls" % "0.0.5")

addSbtPlugin("com.github.gseitz" % "sbt-release" % "0.8.5")
