package ohnosequences.metapasta

import java.io.File


import ohnosequences.awstools.s3.{S3, ObjectAddress, LoadingManager}
import ohnosequences.compota.AnyCompotaConfiguration
import ohnosequences.compota.aws.{AwsCompotaConfiguration, GroupConfiguration}
import ohnosequences.formats.{RawHeader, FASTQ}
import ohnosequences.logging.Logger
import ohnosequences.metapasta.databases._
import ohnosequences.metapasta.instructions.{Blast, FLAShMergingTool, MergingTool, MappingTool}
import ohnosequences.metapasta.reporting.{SampleTag}

import scala.util.Try


case class AssignmentConfiguration(bitscoreThreshold: Int, p: Double = 0.8)

case class QueueThroughput(read: Long, write: Long)


trait MetapastaConfiguration extends AnyCompotaConfiguration {
  type DatabaseReferenceId

  type Database <: Database16S[DatabaseReferenceId]

  def mappingTool(logger: Logger, workingDirectory: File, loadingManager: LoadingManager, database: Database): Try[MappingTool[DatabaseReferenceId, Database]]

  def mappingDatabase(logger: Logger, workingDirectory: File, loadingManager: LoadingManager): Try[Database]

  def taxonRetriever(logger: Logger, workingDirectory: File, loadingManager: LoadingManager): Try[TaxonRetriever[DatabaseReferenceId]]

  def bio4j(logger: Logger, workingDirectory: File, loadingManager: LoadingManager): Try[Bio4j]

  def taxonomyTree(logger: Logger, workingDirectory: File, loadingManager: LoadingManager, bio4j: Bio4j): Try[Tree[Taxon]]

  def readDirectory: ObjectAddress

  def chunksReader(s3: S3, chunk: MergedSampleChunk): List[FASTQ[RawHeader]]

  def flashTemplate: List[String] = List("flash", "$file1$", "$file2")

  def mergingTool(logger: Logger, workingDirectory: File, loadingManager: LoadingManager): Try[MergingTool] = {
    FLAShMergingTool.install(logger, workingDirectory, loadingManager, flashTemplate)
  }

  def notMerged(sample: PairedSample): (ObjectAddress, ObjectAddress)

  def mergedReadsDestination(sample: PairedSample): ObjectAddress

  def fastaWriter(laodingManager: LoadingManager, bio4j: Bio4j): Option[FastasWriter] = Some(new FastasWriter(laodingManager, readDirectory, bio4j))

}

trait AwsMetapastaConfiguration extends MetapastaConfiguration with AwsCompotaConfiguration {

  def mappingWorkers: GroupConfiguration

  def samples: List[PairedSample]

  def tagging: Map[PairedSample, List[SampleTag]]

  def chunksSize: Long

  def chunksThreshold: Option[Int]

  def mergeQueueThroughput: MergeQueueThroughput

  def generateDot: Boolean

  def assignmentConfiguration: AssignmentConfiguration

  def loadingManager(logger: Logger): Try[LoadingManager]

  def mergedSampleQueueThroughput: QueueThroughput

  def pairedSampleQueueThroughput: QueueThroughput

  def readStatsQueueThroughput: QueueThroughput

  def assignTableQueueThroughput: QueueThroughput



}


abstract class MergeQueueThroughput

case class Fixed(n: Int) extends MergeQueueThroughput

case class SampleBased(ration: Double, max: Int = 100) extends MergeQueueThroughput

trait BlastConfiguration extends MetapastaConfiguration {

  override type DatabaseReferenceId = GI

  override type Database = BlastDatabase16S[DatabaseReferenceId]

  def blastTemplate: List[String]

  def mappingTool(logger: Logger, loadingManager: LoadingManager, workingDirectory: File, database: Database): Try[MappingTool[DatabaseReferenceId, Database]] = {
    Blast.linux(logger, loadingManager, workingDirectory, blastTemplate, database)
  }

}

//case class BlastConfiguration(
//                               metadataBuilder: NisperonMetadataBuilder,
//                               mappingWorkers: GroupConfiguration = Group(size = 1, max = 20, instanceType = InstanceType.t1_micro, purchaseModel = OnDemand),
//                               uploadWorkers: Option[Int],
//                               email: String,
//                               samples: List[PairedSample],
//                               tagging: Map[PairedSample, List[SampleTag]] = Map[PairedSample, List[SampleTag]](),
//                               chunksSize: Int = 20000,
//                               chunksThreshold: Option[Int] = None,
//                               blastTemplate: String = """blastn -task megablast -db $db$ -query $input$ -out $output$ -max_target_seqs 1 -num_threads 1 -outfmt $out_format$ -show_gis""",
//                               xmlOutput: Boolean = false,
//                               password: String,
//                               databaseFactory: DatabaseFactory[BlastDatabase16S] = Blast16SFactory,
//                               logging: Boolean = true,
//                               removeAllQueues: Boolean = true,
//                               timeout: Int = 360000,
//                               mergeQueueThroughput: MergeQueueThroughput = SampleBased(1),
//                               generateDot: Boolean = true,
//                               assignmentConfiguration: AssignmentConfiguration = AssignmentConfiguration(400, 0.8),
//                               managerGroupConfiguration: GroupConfiguration = SingleGroup(InstanceType.t1_micro, SpotAuto),
//                               metamanagerGroupConfiguration: GroupConfiguration = SingleGroup(InstanceType.m1_medium, SpotAuto),
//                               defaultInstanceSpecs: InstanceSpecs = NisperonConfiguration.defaultInstanceSpecs,
//                               flashTemplate: String = "flash 1.fastq 2.fastq"
//                               ) extends MetapastaConfiguration {
//}
//
//
//case class LastConfiguration(
//                               metadataBuilder: NisperonMetadataBuilder,
//                               mappingWorkers: GroupConfiguration = Group(size = 1, max = 20, instanceType = InstanceType.m1_medium, purchaseModel = OnDemand),
//                               uploadWorkers: Option[Int],
//                               email: String,
//                               samples: List[PairedSample],
//                               tagging: Map[PairedSample, List[SampleTag]] = Map[PairedSample, List[SampleTag]](),
//                               chunksSize: Int = 2000000,
//                               lastTemplate: String = """./lastal $db$ $input$ -s2 -m100 -T0 -e70 -Q$format$ -f0 -o $output$""",
//                               useFasta: Boolean = true,
//                               chunksThreshold: Option[Int] = None,
//                               databaseFactory: DatabaseFactory[LastDatabase16S] = Last16SFactory,
//                               logging: Boolean = true,
//                               password: String,
//                               removeAllQueues: Boolean = true,
//                               timeout: Int = 360000,
//                               mergeQueueThroughput: MergeQueueThroughput = SampleBased(1),
//                               generateDot: Boolean = true,
//                               assignmentConfiguration: AssignmentConfiguration,
//                               managerGroupConfiguration: GroupConfiguration = SingleGroup(InstanceType.t1_micro, SpotAuto),
//                               metamanagerGroupConfiguration: GroupConfiguration = SingleGroup(InstanceType.m1_medium, SpotAuto),
//                               defaultInstanceSpecs: InstanceSpecs = NisperonConfiguration.defaultInstanceSpecs,
//                               flashTemplate: String = "flash 1.fastq 2.fastq"
//                              ) extends MetapastaConfiguration {
//}


//mappingWorkers = Group(size = 1, max = 20, instanceType = InstanceType.T1Micro, purchaseModel = SpotAuto)