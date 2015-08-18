Nice.scalaProject

name := "metapasta"

description := "metapasta project"

organization := "ohnosequences"

libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.12.4" % "test"

testOptions in Test += Tests.Argument(TestFrameworks.ScalaCheck, "-maxSize", "40", "-minSuccessfulTests", "10", "-workers", "1", "-verbosity", "1")

libraryDependencies ++= Seq(
  "ohnosequences" %% "compota" % "0.9.13-SNAPSHOT",
  "org.apache.commons" % "commons-compress" % "1.9",
  "com.novocode" % "junit-interface" % "0.10" % "test"
)

resolvers += Resolver.url("Statika public ivy releases", url("http://releases.statika.ohnosequences.com.s3.amazonaws.com/"))(ivy)
resolvers +=  Resolver.url("era7" + " public ivy releases",  url("http://releases.era7.com.s3.amazonaws.com"))(Resolver.ivyStylePatterns)
resolvers +=  Resolver.url("era7" + " public ivy snapshots",  url("http://snapshots.era7.com.s3.amazonaws.com"))(Resolver.ivyStylePatterns)

dependencyOverrides += "ohnosequences" %% "aws-scala-tools" % "0.13.2"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.1.2"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.1.2"

dependencyOverrides += "jline" % "jline" % "2.6"

dependencyOverrides += "org.slf4j" % "slf4j-api" % "1.7.5"

dependencyOverrides += "org.apache.httpcomponents" % "httpclient" % "4.3.4"
dependencyOverrides += "commons-logging" % "commons-logging" % "1.1.3"
dependencyOverrides += "commons-codec" % "commons-codec" % "1.6"

