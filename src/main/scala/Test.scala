package meta4

import ohnosequences.awstools.s3.ObjectAddress
import ohnosequences.nisperon.bundles.NisperonMetadataBuilder
import ohnosequences.metapasta._
import ohnosequences.awstools.autoscaling._
import ohnosequences.nisperon._
import ohnosequences.awstools.ec2._
import ohnosequences.metapasta.databases.Blast16SFactory

object mockSamples {
  val testBucket = "metapasta-test"

  val ss1 = "supermock3"
  val s1 = PairedSample(ss1, ObjectAddress(testBucket, "mock/" + ss1 + ".fastq"), ObjectAddress(testBucket, "mock/" + ss1 + ".fastq"))

  val samples = List(s1)
}

import ohnosequences.statika.aws._


class meta4m(
             val organization     : String = "ohnosequences"
             , val artifact         : String = "meta4"
             , val version          : String = "0.1.0-SNAPSHOT"
             , val resolvers        : Seq[String] = Seq("MavenRepository(\"ohnosequences public maven releases\", \"http://releases.frutero.org.s3.amazonaws.com\")", "MavenRepository(\"ohnosequences public maven snapshots\", \"http://snapshots.frutero.org.s3.amazonaws.com\")", "URLRepository(\"ohnosequences public ivy releases\", Patterns(Seq(\"http://releases.frutero.org.s3.amazonaws.com/[organisation]/[module]/(scala_[scalaVersion]/)(sbt_[sbtVersion]/)[revision]/[type]s/[artifact](-[classifier]).[ext]\"), Seq(\"http://releases.frutero.org.s3.amazonaws.com/[organisation]/[module]/(scala_[scalaVersion]/)(sbt_[sbtVersion]/)[revision]/[type]s/[artifact](-[classifier]).[ext]\"), false))", "URLRepository(\"ohnosequences public ivy snapshots\", Patterns(Seq(\"http://snapshots.frutero.org.s3.amazonaws.com/[organisation]/[module]/(scala_[scalaVersion]/)(sbt_[sbtVersion]/)[revision]/[type]s/[artifact](-[classifier]).[ext]\"), Seq(\"http://snapshots.frutero.org.s3.amazonaws.com/[organisation]/[module]/(scala_[scalaVersion]/)(sbt_[sbtVersion]/)[revision]/[type]s/[artifact](-[classifier]).[ext]\"), false))", "MavenRepository(\"Era7 public maven releases\", \"http://releases.era7.com.s3.amazonaws.com\")", "MavenRepository(\"Era7 public maven snapshots\", \"http://snapshots.era7.com.s3.amazonaws.com\")", "URLRepository(\"Statika public ivy releases\", Patterns(Seq(\"http://releases.frutero.org.s3.amazonaws.com/[organisation]/[module]/(scala_[scalaVersion]/)(sbt_[sbtVersion]/)[revision]/[type]s/[artifact](-[classifier]).[ext]\"), Seq(\"http://releases.frutero.org.s3.amazonaws.com/[organisation]/[module]/(scala_[scalaVersion]/)(sbt_[sbtVersion]/)[revision]/[type]s/[artifact](-[classifier]).[ext]\"), false))", "URLRepository(\"Statika public ivy snapshots\", Patterns(Seq(\"http://snapshots.frutero.org.s3.amazonaws.com/[organisation]/[module]/(scala_[scalaVersion]/)(sbt_[sbtVersion]/)[revision]/[type]s/[artifact](-[classifier]).[ext]\"), Seq(\"http://snapshots.frutero.org.s3.amazonaws.com/[organisation]/[module]/(scala_[scalaVersion]/)(sbt_[sbtVersion]/)[revision]/[type]s/[artifact](-[classifier]).[ext]\"), false))", "URLRepository(\"era7 public ivy releases\", Patterns(Seq(\"http://releases.era7.com.s3.amazonaws.com/[organisation]/[module]/(scala_[scalaVersion]/)(sbt_[sbtVersion]/)[revision]/[type]s/[artifact](-[classifier]).[ext]\"), Seq(\"http://releases.era7.com.s3.amazonaws.com/[organisation]/[module]/(scala_[scalaVersion]/)(sbt_[sbtVersion]/)[revision]/[type]s/[artifact](-[classifier]).[ext]\"), false))", "URLRepository(\"era7 public ivy snapshots\", Patterns(Seq(\"http://snapshots.era7.com.s3.amazonaws.com/[organisation]/[module]/(scala_[scalaVersion]/)(sbt_[sbtVersion]/)[revision]/[type]s/[artifact](-[classifier]).[ext]\"), Seq(\"http://snapshots.era7.com.s3.amazonaws.com/[organisation]/[module]/(scala_[scalaVersion]/)(sbt_[sbtVersion]/)[revision]/[type]s/[artifact](-[classifier]).[ext]\"), false))", "MavenRepository(\"sonatype-snapshots\", \"https://oss.sonatype.org/content/repositories/snapshots\")")
             , val privateResolvers : Seq[String] = Seq("S3Resolver(\"Statika private ivy releases\", \"s3://private.releases.frutero.org\", Patterns(Seq(\"[organisation]/[module]/(scala_[scalaVersion]/)(sbt_[sbtVersion]/)[revision]/[type]s/[artifact](-[classifier]).[ext]\"), Seq(\"[organisation]/[module]/(scala_[scalaVersion]/)(sbt_[sbtVersion]/)[revision]/[type]s/[artifact](-[classifier]).[ext]\"), false))", "S3Resolver(\"Statika private ivy snapshots\", \"s3://private.snapshots.frutero.org\", Patterns(Seq(\"[organisation]/[module]/(scala_[scalaVersion]/)(sbt_[sbtVersion]/)[revision]/[type]s/[artifact](-[classifier]).[ext]\"), Seq(\"[organisation]/[module]/(scala_[scalaVersion]/)(sbt_[sbtVersion]/)[revision]/[type]s/[artifact](-[classifier]).[ext]\"), false))", "S3Resolver(\"private.snapshots.frutero.org S3 publishing bucket\", \"s3://private.snapshots.frutero.org\", Patterns(Seq(\"[organisation]/[module]/(scala_[scalaVersion]/)(sbt_[sbtVersion]/)[revision]/[type]s/[artifact](-[classifier]).[ext]\"), Seq(\"[organisation]/[module]/(scala_[scalaVersion]/)(sbt_[sbtVersion]/)[revision]/[type]s/[artifact](-[classifier]).[ext]\"), false))")
             , val artifactUrl      : String = "s3://private.snapshots.frutero.org/ohnosequences/meta4_2.10/0.1.0-SNAPSHOT/jars/meta4_2.10-fat.jar"
             ) extends SbtMetadata with FatJarMetadata



object configuration extends BlastConfiguration (
  metadataBuilder = new NisperonMetadataBuilder(new meta4m()),
  email = "$email$",
  password = "$password$",
  mappingWorkers = Group(size = 10, max = 20, instanceType = InstanceType.T1Micro, purchaseModel = OnDemand),
  uploadWorkers = None,
  samples = mockSamples.samples,
  logging = true,
  databaseFactory = Blast16SFactory,
  xmlOutput = true,
  assignmentConfiguration = AssignmentConfiguration(100, 0.4)
)

object meta4 extends Metapasta(configuration) {

}