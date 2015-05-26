package ohnosequences.metapasta.instructions

import java.io.File

import ohnosequences.awstools.s3.{ObjectAddress, LoadingManager}
import ohnosequences.logging.Logger
import ohnosequences.metapasta.{ReadsStats, S3Paths, ReadStatsBuilder}

import scala.util.Try

abstract class MergingTool {
  val name: String
  def merge(logger: Logger, workingDirectory: File, reads1: File, reads2: File): Try[MergeToolResult]
}

case class MergeStat(total: Long = 0, merged: Long = 0, notMerged: Long = 0)

case class MergeToolResult (
  merged: File,
  notMerged1: Option[File],
  notMerged2: Option[File],
  stats: ReadsStats
)



class FLAShMergingTool(flashTemplate: List[String]) extends MergingTool {

  override val name: String = "flash"

  override def merge(logger: Logger, workingDirectory: File, reads1: File, reads2: File): Try[MergeToolResult] = {
    Try {
      val flashCommand = flashTemplate.map { arg =>
        arg
          .replaceAll("$file1$", reads1.getAbsolutePath)
          .replaceAll("$file2$", reads2.getAbsolutePath)
      }.toSeq

      logger.info("executing FLASh " + flashCommand)

      val flashOut = sys.process.Process(flashCommand, workingDirectory).!!

      //[FLASH] Read combination statistics:
      //[FLASH]     Total reads:      334434
      //[FLASH]     Combined reads:   984
      //[FLASH]     Uncombined reads: 333450
      val totalRe = """\Q[FLASH]\E\s+\QTotal reads:\E\s+(\d+)""".r
      val combinedRe = """\Q[FLASH]\E\s+\QCombined reads:\E\s+(\d+)""".r
      val uncombinedRe = """\Q[FLASH]\E\s+\QUncombined reads:\E\s+(\d+)""".r

      val readsStats = new ReadStatsBuilder()

      val mergeStat = MergeStat()

      flashOut.split("\n").foreach {
        case totalRe(n) => readsStats.total = n.toLong
        case combinedRe(n) => readsStats.merged = n.toLong
        case uncombinedRe(n) => readsStats.notMerged = n.toLong
        case _ =>
      }

      val mergedFile = new File(workingDirectory, "out.extendedFrags.fastq")
      val notMerged1 = Some(new File(workingDirectory, "out.notCombined_1.fastq"))
      val notMerged2 = Some(new File(workingDirectory, "out.notCombined_2.fastq"))

      MergeToolResult(mergedFile, notMerged1, notMerged2, readsStats.build)
    }
  }
}

object FLAShMergingTool {
  def install(logger: Logger, workingDirectory: File, loadingManager: LoadingManager, flashTemplate: List[String]): Try[FLAShMergingTool] = {
    Try {
      val flash = "flash"
      val flashDst = new File("/usr/bin", flash)
      loadingManager.download(ObjectAddress("metapasta", flash), flashDst)
      flashDst.setExecutable(true)
      new FLAShMergingTool(flashTemplate)
    }
  }
}