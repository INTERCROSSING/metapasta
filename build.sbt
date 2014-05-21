Nice.scalaProject

name := "metapasta"

description := "metapasta project"

organization := "ohnosequences"


libraryDependencies ++= Seq(
  "ohnosequences" % "compota_2.10" % "0.8.4-SNAPSHOT",
  "ohnosequences" % "bio4j-ncbi-taxonomy_2.10" % "0.1.0"  classifier("")
)


resolvers += Resolver.url("Statika public ivy releases", url("http://releases.statika.ohnosequences.com.s3.amazonaws.com/"))(ivy)

resolvers +=  Resolver.url("era7" + " public ivy releases",  url("http://releases.era7.com.s3.amazonaws.com"))(Resolver.ivyStylePatterns)

resolvers +=  Resolver.url("era7" + " public ivy snapshots",  url("http://snapshots.era7.com.s3.amazonaws.com"))(Resolver.ivyStylePatterns)

dependencyOverrides += "ohnosequences" % "aws-scala-tools_2.10" % "0.7.1-SNAPSHOT"

dependencyOverrides += "ohnosequences" % "aws-statika_2.10" % "1.0.1"

dependencyOverrides += "ohnosequences" % "amazon-linux-ami_2.10" % "0.14.1"

dependencyOverrides += "commons-codec" % "commons-codec" % "1.6"

//dependencyOverrides += "org.scala-lang" % "scala-library" % "2.10.4"

//dependencyOverrides += "org.scala-lang" % "scala-compiler" % "2.10.4"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.1.2"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.1.2"

dependencyOverrides += "jline" % "jline" % "2.6"

dependencyOverrides += "org.slf4j" % "slf4j-api" % "1.7.5"

