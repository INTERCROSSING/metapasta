package ohnosequences.metapasta

import java.io.File

import ohnosequences.awstools.s3.{LoadingManager, ObjectAddress}
import ohnosequences.logging.Logger
import ohnosequences.metapasta.databases.{BlastDatabase, GI, Installable, TaxonRetriever}
import ohnosequences.metapasta.instructions.{Blast, FLAShMergingTool, MappingTool, MergingTool}
import ohnosequences.metapasta.reporting.SampleTag
import org.junit.Assert._
import org.junit.Test

import scala.concurrent.duration._
import scala.util.{Success, Try}

class LocalMetapastaTest extends MetapastaTest {


  @Test
  def localMetapasta(): Unit = {
    launch("localMetapasta", false) { case (logger2, aws2, loadingManager2) =>
      object LocalMetapastaConfiguration extends LocalMetapastaConfiguration with BlastConfiguration {


        override def chunksSize: Long = 10000

        override def workingDirectory: File = new File(new File("test"), "localMetapasta")

        def readsDestination(sampleName: String): ObjectAddress = s3location / "local_metapasta" / "reads" / sampleName

        override def mergedReadsDestination(sample: PairedSample): ObjectAddress = readsDestination(sample.name) / "merged.fastq"

        override def loadingManager(logger: Logger): Try[LoadingManager] = Success(loadingManager2)

        override def assignmentConfiguration: AssignmentConfiguration = AssignmentConfiguration(400, 0.8)

        override def notMergedReadsDestination(sample: PairedSample): (ObjectAddress, ObjectAddress) = {
          (readsDestination(sample.name) / "notMerged1.fastq", readsDestination(sample.name) / "notMerged2.fastq")
        }

        override def mergingTool(logger: Logger, workingDirectory: File, loadingManager: LoadingManager): Try[MergingTool] = {
          FLAShMergingTool.windows(logger, new File("test", "flash"), loadingManager, flashTemplate)
        }


        override val taxonRetriever
        : Installable[TaxonRetriever[LocalMetapastaConfiguration.DatabaseReferenceId]] = new Installable[TaxonRetriever[LocalMetapastaConfiguration.DatabaseReferenceId]] {
          val inMemory = TaxonRetriever.inMemory

          override def install(logger: Logger, workingDirectory: File, loadingManager: LoadingManager)
          : Try[TaxonRetriever[LocalMetapastaConfiguration.DatabaseReferenceId]] = {
            inMemory.get(logger, new File("test", "taxonRetriver"), loadingManager)
          }

        }

        override val bio4j: Installable[Bio4j] = new Installable[Bio4j] {
          override def install(logger: Logger, workingDirectory: File, loadingManager: LoadingManager): Try[Bio4j] = {
            Bio4j.bio4j201401.get(logger, new File("test", "bio4j"), loadingManager)
          }
        }

        override val mappingDatabase: Installable[Database] = new Installable[Database] {

          val database = BlastDatabase.march2014database

          override def install(logger: Logger, workingDirectory: File, loadingManager: LoadingManager): Try[LocalMetapastaConfiguration.Database] = {
            database.get(logger, new File("test", "blastDatabase"), loadingManager)
          }
        }

        override val mappingTool: Installable[MappingTool[DatabaseReferenceId, Database]]
        = new Installable[MappingTool[LocalMetapastaConfiguration.DatabaseReferenceId, LocalMetapastaConfiguration.Database]] {
          val blast = Blast.windows[GI](Blast.defaultBlastnTemplate)

          override def install(logger: Logger, workingDirectory: File, loadingManager: LoadingManager):
          Try[MappingTool[LocalMetapastaConfiguration.DatabaseReferenceId, LocalMetapastaConfiguration.Database]] = {
            blast.get(logger, new File("test", "blast"), loadingManager)
          }
        }

        override def samples: List[PairedSample] = List(PairedSample("testSample", s3location / "test1s.fastq", s3location / "test2s.fastq.gz"))

        override def mergers: Int = 1

        override def mappers: Int = 4

        override def fastaWriter: Installable[Option[FastasWriter]] = new Installable[Option[FastasWriter]] {
          override def install(logger: Logger, workingDirectory: File, loadingManager: LoadingManager): Try[Option[FastasWriter]] = Success(None)
        }

        override def tagging: Map[PairedSample, List[SampleTag]] = Map()

        override def chunksThreshold: Option[Int] = Some(4)

        override def generateDot: Boolean = false

        override def name: String = "local_metapasta"

        override def loggerDebug: Boolean = true

        override def timeout: Duration = Duration(300, SECONDS)
      }

      object LocalMetapastaTest extends LocalMetapasta(LocalMetapastaConfiguration)

      val graph = LocalMetapastaTest.nisperoGraph
      logger2.info("not leaf queues: " + graph.notLeafsQueues)
      logger2.info("edges:" + graph.graph.edges)
      logger2.info("sorted queues: " + graph.sortedQueues)
      assertEquals("nispero graph size check", 4, graph.graph.edges.size)
      graph.graph.edges.foreach {
        case edge if edge.label.equals("merge_0_0") => {
          assertEquals("nispero merge edge check", LocalMetapastaTest.pairedSamplesQueue.name, edge.source.label)
          assertEquals("nispero merge edge check", LocalMetapastaTest.mergedSampleChunksQueue.name, edge.target.label)
        }
        case edge if edge.label.equals("merge_0_1") => {
          assertEquals("nispero merge edge check", LocalMetapastaTest.pairedSamplesQueue.name, edge.source.label)
          assertEquals("nispero merge edge check", LocalMetapastaTest.readsStatsQueue.name, edge.target.label)
        }
        case edge if edge.label.equals("map_0_0") => {
          assertEquals("nispero map edge check", LocalMetapastaTest.mergedSampleChunksQueue.name, edge.source.label)
          assertEquals("nispero map edge check", LocalMetapastaTest.assignTableQueue.name, edge.target.label)
        }
        case edge if edge.label.equals("map_0_1") => {
          assertEquals("nispero map edge check", LocalMetapastaTest.mergedSampleChunksQueue.name, edge.source.label)
          assertEquals("nispero map edge check", LocalMetapastaTest.readsStatsQueue.name, edge.target.label)
        }
        case edge => fail("nispero map edge check: unexpected edge " + edge)
      }
      assertEquals("queue order check", List(
        LocalMetapastaTest.pairedSamplesQueue.name,
        LocalMetapastaTest.mergedSampleChunksQueue.name,
        LocalMetapastaTest.readsStatsQueue.name,
        LocalMetapastaTest.assignTableQueue.name), graph.sortedQueues.map(_.label)
      )
      assertEquals("not leaf queues check",
        List(LocalMetapastaTest.pairedSamplesQueue.name,
          LocalMetapastaTest.mergedSampleChunksQueue.name),
        graph.notLeafsQueues
      )

            LocalMetapastaTest.launch().map { env =>
              LocalMetapastaTest.waitForFinished()
            }
     // Success(())
      //graph.
      //  LocalMetapastaTest.waitForFinished()
    }
  }
}
