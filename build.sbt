Statika.distributionProject

name := "metapasta"

description := "metapasta project"

organization := "ohnosequences"

metadataObject := name.value

libraryDependencies ++= Seq(
  "commons-io"     % "commons-io" % "2.4",
  "com.novocode"   % "junit-interface" % "0.10" % "test",
  "org.clapper"   %% "avsl" % "1.0.1",
  "org.json4s"    %% "json4s-native" % "3.2.5",
  "ohnosequences" %% "aws-scala-tools" % "0.4.3",
  "ohnosequences" %% "statika" % "1.0.0",
  "ohnosequences" %% "aws-statika" % "1.0.0",
  "ohnosequences" %% "amazon-linux-ami" % "0.14.1"
)

bucketSuffix := "frutero.org"

dependencyOverrides += "commons-codec" % "commons-codec" % "1.6"

dependencyOverrides += "org.scala-lang" % "scala-library" % "2.10.3"

dependencyOverrides += "org.scala-lang" % "scala-compiler" % "2.10.3"
