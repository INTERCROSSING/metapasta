Statika.distributionProject

name := "metapasta"

description := "metapasta project"

organization := "ohnosequences"

metadataObject := name.value

libraryDependencies ++= Seq(
  "commons-io"     % "commons-io" % "2.1",
  "com.novocode"   % "junit-interface" % "0.10" % "test",
  "org.clapper"   %% "avsl" % "1.0.1",
  "org.json4s"    %% "json4s-native" % "3.2.5",
  "ohnosequences" %% "aws-scala-tools" % "0.5.0",
  "ohnosequences" %% "statika" % "1.0.0",
  "ohnosequences" %% "aws-statika" % "1.0.0",
  "ohnosequences" %% "amazon-linux-ami" % "0.14.1"
 // "ohnosequences" % "bio4j-scala-distribution_2.10" % "0.1.0-SNAPSHOT"
)

resolvers +=  Resolver.url("era7" + " public ivy releases",  url("http://snapshots.era7.com.s3.amazonaws.com"))(Resolver.ivyStylePatterns)

bucketSuffix := "frutero.org"

dependencyOverrides += "commons-codec" % "commons-codec" % "1.6"

dependencyOverrides += "org.scala-lang" % "scala-library" % "2.10.3"

dependencyOverrides += "org.scala-lang" % "scala-compiler" % "2.10.3"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.1.2"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.1.2"

dependencyOverrides += "jline" % "jline" % "2.6"

dependencyOverrides += "org.slf4j" % "slf4j-api" % "1.7.5"

