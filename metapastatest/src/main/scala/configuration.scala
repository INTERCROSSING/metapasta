package metapastatest

import ohnosequences.awstools.s3.ObjectAddress
import ohnosequences.nisperon.bundles.NisperonMetadataBuilder
import ohnosequences.metapasta._
import ohnosequences.awstools.autoscaling._
import ohnosequences.nisperon._
import ohnosequences.awstools.ec2._

object mockSamples {
  val testBucket = "metapasta-test"

  val ss1 = "SRR172902"
  val s1 = PairedSample(ss1, ObjectAddress(testBucket, "mock/" + ss1 + ".fastq"), ObjectAddress(testBucket, "mock/" + ss1 + ".fastq"))

  val ss2 = "SRR172903"
  val s2 = PairedSample(ss2, ObjectAddress(testBucket, "mock/" + ss2 + ".fastq"), ObjectAddress(testBucket, "mock/" + ss2 + ".fastq"))

  val samples = List(s1, s2)
}

object configuration extends BlastConfiguration (
  metadataBuilder = new NisperonMetadataBuilder(new generated.metadata.metapastatest()),
  email = "museeer@gmail.com",
  mappingWorkers = Group(size = 10, max = 20, instanceType = InstanceType.T1Micro, purchaseModel = OnDemand),
  uploadWorkers = None,
  samples = mockSamples.samples,
  logging = true,
  database = NTDatabase,
  xmlOutput = true
)

object metapastatest extends Metapasta(configuration) {


}
