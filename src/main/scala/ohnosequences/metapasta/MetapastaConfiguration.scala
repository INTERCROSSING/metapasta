package ohnosequences.metapasta

import java.io.File

import ohnosequences.awstools.s3.ObjectAddress
import ohnosequences.compota.bundles.NisperonMetadataBuilder
import ohnosequences.compota.{CompotaConfiguration, SingleGroup, GroupConfiguration}
import ohnosequences.awstools.ec2.{InstanceSpecs, InstanceType}
import ohnosequences.awstools.autoscaling.{SpotAuto}
import ohnosequences.metapasta.databases._
import ohnosequences.metapasta.instructions._
import ohnosequences.metapasta.reporting.{SampleTag}


case class AssignmentConfiguration(bitscoreThreshold: Int, p: Double = 0.8)


trait MetapastaConfiguration extends MappingInstructionsConfiguration with MergingInstructionsConfiguration {
  val metadataBuilder: NisperonMetadataBuilder
  val mappingWorkers: GroupConfiguration
  val email: String
  val password: String
  val samples: List[PairedSample]
  val tagging: Map[PairedSample, List[SampleTag]]
  val timeout: Int
  val generateDot: Boolean = false

  val removeAllQueues: Boolean = true
  val logging: Boolean = true
  val mergeQueueThroughput: MergeQueueThroughput = SampleBased(1)
  val taxonomy: Installable[Taxonomy] = new OhnosequencesNCBITaxonomy()
  val workingDirectory: File = new File("/media/ephemeral0/metapasta")
  val metamanagerGroupConfiguration: GroupConfiguration = SingleGroup(InstanceType.m1_medium, SpotAuto)
  val managerGroupConfiguration: GroupConfiguration = SingleGroup(InstanceType.m1_medium, SpotAuto)
  val defaultInstanceSpecs: InstanceSpecs = CompotaConfiguration.defaultInstanceSpecs

  def readsDestination: ObjectAddress

  override val mergedReadsDestination: (PairedSample) => ObjectAddress
  override val notMergedReadsDestination: (PairedSample) => (ObjectAddress, ObjectAddress)
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

  val chunksThreshold: Option[Int]

  val chunksSize: Long

  val mergedReadsDestination: PairedSample => ObjectAddress

  val notMergedReadsDestination: PairedSample => (ObjectAddress, ObjectAddress)

}

trait FlashConfiguration extends MergingInstructionsConfiguration {
  override val mergingTool: Installable[MergingTool] = new Era7FLASh()
}

