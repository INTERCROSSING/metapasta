Nice.scalaProject

name := "metapasta"

description := "metapasta project"

organization := "ohnosequences"

libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.11.0" % "test"

testOptions in Test += Tests.Argument(TestFrameworks.ScalaCheck, "-maxSize", "40", "-minSuccessfulTests", "10", "-workers", "1", "-verbosity", "1")

libraryDependencies ++= Seq(
  "ohnosequences" %% "compota" % "0.10.0-SNAPSHOT",
  "ohnosequences" % "bio4j-titandb" % "0.3.1" exclude("com.amazonaws", "aws-java-sdk"),
  "com.novocode" % "junit-interface" % "0.10" % "test"
)

//dependencyOverrides += "com.amazonaws" % "aws-java-sdk" % "1.9.25"

dependencyOverrides += "org.apache.httpcomponents" % "httpclient" % "4.3.4"

dependencyOverrides += "ohnosequences" % "aws-scala-tools_2.10" % "0.13.0-SNAPSHOT"

dependencyOverrides += "commons-logging" % "commons-logging" % "1.1.3"

dependencyOverrides += "commons-codec" % "commons-codec" % "1.6"

dependencyOverrides += "commons-io" % "commons-io" % "2.4"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.1.2"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.1.2"

dependencyOverrides += "org.scala-lang.modules" % "scala-parser-combinators_2.11" % "1.0.2"

dependencyOverrides += "org.scala-lang.modules" % "scala-xml_2.11" % "1.0.2"

dependencyOverrides += "jline" % "jline" % "2.6"

dependencyOverrides += "org.slf4j" % "slf4j-api" % "1.7.5"

