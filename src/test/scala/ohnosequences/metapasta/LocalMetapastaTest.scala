package ohnosequences.metapasta

import ohnosequences.awstools.s3.{LoadingManager, ObjectAddress, S3}
import ohnosequences.formats.{RawHeader, FASTQ}
import ohnosequences.logging.Logger
import ohnosequences.metapasta.databases.Installable
import ohnosequences.metapasta.reporting.SampleTag
import org.junit.Test

import scala.concurrent.duration.Duration
import scala.util.{Success, Try}
import scala.util.parsing.combinator.Parsers.Success


class LocalMetapastaTest extends MetapastaTest {

  

  @Test
  def localMetapasta(): Unit = {
    def flashTool(): Unit = {
      launch("flashTool", false) { case (logger2, aws2, loadingManager2) =>
        object LocalMetapastaConfiguration extends LocalMetapastaConfiguration with BlastConfiguration {

          override def mergedReadsDestination(sample: PairedSample): ObjectAddress = s3location / "local_metapasta" / "reads" / sample.name / "merged.fastq"

          override def loadingManager(logger: Logger): Try[LoadingManager] = Success(loadingManager2)

          override def taxonomyTree: Installable[Tree[Taxon]] = ???

          override def mergeQueueThroughput: MergeQueueThroughput = ???

          override def assignmentConfiguration: AssignmentConfiguration = ???

          override def notMergedReadsDestination(sample: PairedSample): (ObjectAddress, ObjectAddress) = ???

          override def chunksSize: Long = ???

          override def samples: List[PairedSample] = ???

          override def mergers: Int = ???

          override def readDirectory: ObjectAddress = ???

          override def mappers: Int = ???

          override def fastaWriter: Installable[Option[FastasWriter]] = ???

          override def tagging: Map[PairedSample, List[SampleTag]] = ???

          override def chunksThreshold: Option[Int] = ???

          override def generateDot: Boolean = ???

          override def chunksReader(s3: S3, chunk: MergedSampleChunk): List[FASTQ[RawHeader]] = ???

          override def name: String = ???

          override def loggerDebug: Boolean = ???

          override def timeout: Duration = ???
        }


      }

  }


}
