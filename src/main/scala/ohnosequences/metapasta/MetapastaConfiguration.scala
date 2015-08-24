package ohnosequences.metapasta

import java.io.File

import ohnosequences.awstools.s3.ObjectAddress
import ohnosequences.compota.bundles.NisperonMetadataBuilder
import ohnosequences.compota.{Naming, CompotaConfiguration, SingleGroup, GroupConfiguration}
import ohnosequences.awstools.ec2.{InstanceSpecs, InstanceType}
import ohnosequences.awstools.autoscaling.{SpotAuto}
import ohnosequences.metapasta.databases._
import ohnosequences.metapasta.instructions._

import scala.concurrent.duration.Duration


case class AssignmentConfiguration(bitscoreThreshold: Int, p: Double = 0.8)


trait MetapastaConfiguration extends MergingInstructionsConfiguration with MappingInstructionsConfiguration with S3Paths {
  val metadataBuilder: NisperonMetadataBuilder
  val mappingWorkers: GroupConfiguration
  val email: String
  val password: String
  val samples: List[PairedSample]
  val tagging: Map[PairedSample, List[SampleTag]]
  val timeout: Duration
  val generateDot: Boolean = false

  val removeAllQueues: Boolean = true
  val logging: Boolean = true
  val mergeQueueThroughput: MergeQueueThroughput = SampleBased(1)
  val taxonomy: Installable[Taxonomy] = new OhnosequencesNCBITaxonomy()
  val workingDirectory: File = new File("/media/ephemeral0/metapasta")
  val metamanagerGroupConfiguration: GroupConfiguration = SingleGroup(InstanceType.m1_medium, SpotAuto)
  val managerGroupConfiguration: GroupConfiguration = SingleGroup(InstanceType.m1_medium, SpotAuto)
  val defaultInstanceSpecs: InstanceSpecs = CompotaConfiguration.defaultInstanceSpecs

}

trait S3Paths {
  s3Paths: MetapastaConfiguration =>

  def resultsBucket: String = Naming.s3name(metadataBuilder.id)

  def resultsDestination: ObjectAddress = ObjectAddress(resultsBucket, "results")

  def resultsReadsDestination(sampleId: SampleId): ObjectAddress = resultsDestination / sampleId.id / "reads"

  def mergedReadsDestination(sample: SampleId): ObjectAddress = {
    resultsReadsDestination(sample) / (sample.id + ".merged.fastq")
  }

  def notMergedReadsDestination(sample: SampleId): (ObjectAddress, ObjectAddress) = {
    (readsDestination(sample) / (sample.id + ".notMerged1.fastq"), readsDestination(sample) / (sample.id + ".notMerged2.fastq"))
  }

  def readsDestination: ObjectAddress = ObjectAddress(resultsBucket, "reads")

  def readsDestination(sample: SampleId): ObjectAddress = readsDestination / sample.id

  def readsDestination(sample: SampleId, assignmentType: AssignmentType): ObjectAddress = readsDestination / (sample.id + "###" + assignmentType)

  def noHitFastas(sample: SampleId): ObjectAddress = readsDestination(sample) / "noHit"

  def noHitFasta(chunk: ChunkId): ObjectAddress = {
    noHitFastas(chunk.sample) / (chunk.start + "_" + chunk.end + ".fasta")
  }

  def noTaxIdFastas(sample: SampleId, assignmentType: AssignmentType): ObjectAddress = readsDestination(sample, assignmentType) / "noTaxId"

  def noTaxIdFasta(chunk: ChunkId, assignmentType: AssignmentType): ObjectAddress = {
    noTaxIdFastas(chunk.sample, assignmentType) / (chunk.sample.id + chunk.start + "_" + chunk.end + ".fasta")
  }

  def notAssignedFastas(sample: SampleId, assignmentType: AssignmentType): ObjectAddress = readsDestination(sample, assignmentType) / "notAssigned"

  def notAssignedFasta(chunk: ChunkId, assignmentType: AssignmentType): ObjectAddress = {
    notAssignedFastas(chunk.sample, assignmentType) / (chunk.sample.id + chunk.start + "_" + chunk.end + ".fasta")
  }

  def assignedFastas(sample: SampleId, assignmentType: AssignmentType): ObjectAddress = readsDestination(sample, assignmentType) / "assigned"

  def assignedFasta(chunk: ChunkId, assignmentType: AssignmentType): ObjectAddress = {
    assignedFastas(chunk.sample, assignmentType) / (chunk.sample.id + chunk.start + "_" + chunk.end + ".fasta")
  }

  def mergedNoHitFasta(sample: SampleId): ObjectAddress = resultsReadsDestination(sample) / (sample + ".noHit.fasta")

  def mergedNoTaxIdFasta(sample: SampleId, assignmentType: AssignmentType): ObjectAddress = resultsReadsDestination(sample) / (sample + "." + assignmentType + ".noTaxId.fasta")

  def mergedAssignedFasta(sample: SampleId, assignmentType: AssignmentType): ObjectAddress = resultsReadsDestination(sample) / (sample + "." + assignmentType + ".assigned.fasta")

  def mergedNotAssignedFasta(sample: SampleId, assignmentType: AssignmentType): ObjectAddress = resultsReadsDestination(sample) / (sample + "." + assignmentType + ".notAssigned.fasta")

}

abstract class MergeQueueThroughput

case class Fixed(n: Int) extends MergeQueueThroughput

case class SampleBased(ration: Double, max: Int = 100) extends MergeQueueThroughput


trait BlastConfiguration extends MetapastaConfiguration {

  //override val assignmentConfiguration: AssignmentConfiguration = AssignmentConfiguration(400, 0.8)

  override val chunksSize: Long = 20000

  val xmlOutput: Boolean = false

  override type DatabaseReferenceId = GI
  override type Database = BlastDatabase16S[GI]
  val database: Installable[BlastDatabase16S[GI]] = new OhnosequencesBlastDatabase16()

  override val mappingTool: Installable[MappingTool[GI, BlastDatabase16S[GI]]] = Blast.linux(blastTemplate)

  override val taxonRetriever: Installable[TaxonRetriever[GI]] = new OhnosequencesGIMapper()

  //override val fastaWriter: Installable[Option[FastasWriter]] = new FastasWriter[DatabaseReferenceId]()

  def blastTemplate: List[String] = List(
    "-task",
    "megablast",
    "-db",
    "$db$",
    "-query",
    "$input$",
    "-out",
    "$output$",
    "-max_target_seqs",
    "1",
    "-num_threads",
    "$threads_count$",
    "-outfmt",
    "$out_format$",
    "-show_gis"
  )

  //override val mappingWorkers: GroupConfiguration = Group(size = 1, max = 20, instanceType = InstanceType.t1_micro, purchaseModel = OnDemand)
}

//trait LastConfiguration extends MetapastaConfiguration with MappingInstructionsConfiguration {
//  //override val assignmentConfiguration: AssignmentConfiguration = AssignmentConfiguration(400, 0.8)
//  override val chunksSize: Int = 2000000
//  override type DatabaseReferenceId = GI
//  override type Database = LastDatabase16S[GI]
//  val database: Installable[LastDatabase16S[GI]] = new OhnosequencesLastDatabase16()
//
//  def lastTemplate: String = """./lastal $db$ $input$ -s2 -m100 -T0 -e70 -Q$format$ -f0 -o $output$"""
//  val useFasta: Boolean = true
//  //override val mappingWorkers: GroupConfiguration = Group(size = 1, max = 20, instanceType = InstanceType.m1_medium, purchaseModel = OnDemand)
//}


trait MappingInstructionsConfiguration {

  type DatabaseReferenceId <: ReferenceId

  type Database <: Database16S[DatabaseReferenceId]

  val mappingTool: Installable[MappingTool[DatabaseReferenceId, Database]]

  val taxonomy: Installable[Taxonomy]

  val taxonRetriever: Installable[TaxonRetriever[DatabaseReferenceId]]

  //val fastaWriter: Installable[Option[FastasWriter]]

  val database: Installable[Database]

  val assignmentConfiguration: AssignmentConfiguration

}


trait MergingInstructionsConfiguration {

  val mergingTool: Installable[MergingTool]

  val chunksThreshold: Option[Int] = None

  val chunksSize: Long


}

trait FlashConfiguration extends MergingInstructionsConfiguration {

  val flashTemplate: List[String] = List("$file1$", "$file2$")

  override val mergingTool: Installable[MergingTool] = new Era7FLASh(commandTemplate = flashTemplate)


}

