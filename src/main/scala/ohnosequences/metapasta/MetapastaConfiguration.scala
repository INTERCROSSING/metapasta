package ohnosequences.metapasta


import ohnosequences.awstools.s3.{S3, ObjectAddress, LoadingManager}
import ohnosequences.compota.AnyCompotaConfiguration
import ohnosequences.compota.aws.{AwsCompotaConfiguration, GroupConfiguration}
import ohnosequences.compota.local.{AnyLocalCompotaConfiguration}
import ohnosequences.formats.{RawHeader, FASTQ}
import ohnosequences.logging.Logger
import ohnosequences.metapasta.databases._
import ohnosequences.metapasta.instructions.{Blast, FLAShMergingTool, MergingTool, MappingTool}
import ohnosequences.metapasta.reporting.{SampleTag}
import scala.util.{Try}
import java.io.File


case class AssignmentConfiguration(bitscoreThreshold: Int, p: Double = 0.8)

case class QueueThroughput(read: Long, write: Long)


trait MetapastaConfiguration extends AnyCompotaConfiguration { metapastaConfiguration =>
  type DatabaseReferenceId <: ReferenceId

  type Database <: Database16S[DatabaseReferenceId]

  def loadingManager(logger: Logger): Try[LoadingManager]

  def mergers: Int

  def mappers: Int

  def mappingTool: Installable[MappingTool[DatabaseReferenceId, Database]]

  def mappingDatabase: Installable[Database]

  def taxonRetriever: Installable[TaxonRetriever[DatabaseReferenceId]]

  def taxonomyTree: Installable[Tree[Taxon]]

  def fastaWriter: Installable[Option[FastasWriter]]

  def readDirectory: ObjectAddress

  def chunksReader(s3: S3, chunk: MergedSampleChunk): List[FASTQ[RawHeader]]

  def flashTemplate: List[String] = List("flash", "$file1$", "$file2")

  def mergingTool(logger: Logger, workingDirectory: File, loadingManager: LoadingManager): Try[MergingTool] = {
    FLAShMergingTool.linux(logger, workingDirectory, loadingManager, flashTemplate)
  }

  def notMergedReadsDestination(sample: PairedSample): (ObjectAddress, ObjectAddress)

  def mergedReadsDestination(sample: PairedSample): ObjectAddress

  def assignmentConfiguration: AssignmentConfiguration

  def samples: List[PairedSample]

  def tagging: Map[PairedSample, List[SampleTag]]

  def chunksSize: Long

  def chunksThreshold: Option[Int]

  def mergeQueueThroughput: MergeQueueThroughput

  def generateDot: Boolean

  def mergingInstructionsConfiguration: MergingInstructionsConfiguration = MergingInstructionsConfiguration(
    loadingManager = loadingManager,
    mergingTool = mergingTool,
    mergedReadsDestination = mergedReadsDestination,
    notMergedReadsDestination = notMergedReadsDestination,
    chunksThreshold = chunksThreshold,
    chunksSize = chunksSize
  )

  def mappingInstructionsConfiguration: MappingInstructionsConfiguration[DatabaseReferenceId, Database] = {
    MappingInstructionsConfiguration(
      loadingManager = loadingManager,
      taxonomyTree = taxonomyTree,
      taxonRetriever = taxonRetriever,
      fastaWriter = fastaWriter,
      database = mappingDatabase,
      mappingTool = mappingTool,
      assignmentConfiguration = assignmentConfiguration
    )
  }


}

trait Bio4jConfiguration extends MetapastaConfiguration {

  def bio4j: Installable[Bio4j]

  override val taxonomyTree: Installable[Tree[Taxon]] = new Installable[Tree[Taxon]] {
    override def install(logger: Logger, workingDirectory: File, loadingManager: LoadingManager): Try[Tree[Taxon]] = {
      bio4j.get(logger, workingDirectory, loadingManager).map { bio4j =>
        new Bio4JTaxonomyTree(bio4j)
      }
    }
  }
}


case class MergingInstructionsConfiguration( loadingManager: Logger => Try[LoadingManager],
                                           mergingTool: (Logger, File, LoadingManager) => Try[MergingTool],
                                           mergedReadsDestination: PairedSample => ObjectAddress,
                                           notMergedReadsDestination: PairedSample => (ObjectAddress, ObjectAddress),
                                           chunksThreshold: Option[Int],
                                           chunksSize: Long
                                           )



trait AnyMappingInstructionsConfiguration {

  type DatabaseReferenceId <: ReferenceId

  type Database <: Database16S[DatabaseReferenceId]

  val loadingManager: Logger => Try[LoadingManager]

  val mappingTool: Installable[MappingTool[DatabaseReferenceId, Database]]

  val taxonomyTree: Installable[Tree[Taxon]]

  val taxonRetriever: Installable[TaxonRetriever[DatabaseReferenceId]]

  val fastaWriter: Installable[Option[FastasWriter]]

  val database: Installable[Database]

  val assignmentConfiguration: AssignmentConfiguration

}





case class MappingInstructionsConfiguration[R <: ReferenceId, D <: Database16S[R]](
                                                                               loadingManager: Logger => Try[LoadingManager],
                                                                               mappingTool: Installable[MappingTool[R, D]],
                                                                               taxonomyTree: Installable[Tree[Taxon]],
                                                                               taxonRetriever: Installable[TaxonRetriever[R]],
                                                                               fastaWriter: Installable[Option[FastasWriter]],
                                                                               database: Installable[D],
                                                                               assignmentConfiguration: AssignmentConfiguration
                                                                               ) extends AnyMappingInstructionsConfiguration{
    override type DatabaseReferenceId = R
    override type Database = D
}


trait LocalMetapastaConfiguration extends MetapastaConfiguration with AnyLocalCompotaConfiguration {

}


trait AwsMetapastaConfiguration extends MetapastaConfiguration with AwsCompotaConfiguration {

  def mappingWorkers: GroupConfiguration

  def mergedSampleQueueThroughput: QueueThroughput

  def pairedSampleQueueThroughput: QueueThroughput

  def readStatsQueueThroughput: QueueThroughput

  def assignTableQueueThroughput: QueueThroughput

}


abstract class MergeQueueThroughput

case class Fixed(n: Int) extends MergeQueueThroughput

case class SampleBased(ration: Double, max: Int = 100) extends MergeQueueThroughput


trait BlastConfiguration extends MetapastaConfiguration { metapastaConfiguration =>

  override type DatabaseReferenceId = GI

  override type Database = BlastDatabase16S[GI]

  def blastTemplate: List[String] = Blast.defaultBlastnTemplate

  override val taxonRetriever: Installable[TaxonRetriever[DatabaseReferenceId]] = TaxonRetriever.inMemory

  override val mappingDatabase: Installable[Database] = BlastDatabase.march2014database

  override val mappingTool: Installable[MappingTool[DatabaseReferenceId, Database]] = Blast.linux[GI](Blast.defaultBlastnTemplate)


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