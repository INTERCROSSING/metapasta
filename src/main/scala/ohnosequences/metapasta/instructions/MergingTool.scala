package ohnosequences.metapasta.instructions

import java.io.File

import ohnosequences.awstools.s3.{LoadingManager, ObjectAddress}
import ohnosequences.logging.Logger
import ohnosequences.metapasta.{ReadStatsBuilder, ReadsStats}

import scala.util.{Failure, Try}

abstract class MergingTool {
  val name: String

  def merge(logger: Logger, workingDirectory: File, reads1: File, reads2: File): Try[MergeToolResult]
}

case class MergeStat(total: Long = 0, merged: Long = 0, notMerged: Long = 0)

case class MergeToolResult(
                            merged: File,
                            notMerged1: Option[File],
                            notMerged2: Option[File],
                            stats: ReadsStats
                            )


class FLAShMergingTool(flashLocation: File, flashTemplate: List[String]) extends MergingTool {

  override val name: String = "flash"

  override def merge(logger: Logger, workingDirectory: File, reads1: File, reads2: File): Try[MergeToolResult] = {
    Try {
      val flashCommand = List(flashLocation.getAbsolutePath) ++ flashTemplate.map { arg =>
        arg
          .replace("$file1$", reads1.getAbsolutePath)
          .replace("$file2$", reads2.getAbsolutePath)
      }.toSeq

      logger.info("executing FLASh " + flashCommand)

      val flashOut = sys.process.Process(flashCommand, workingDirectory).!!(logger.processLogger)

      logger.debug("flash output: " + flashOut)

      //[FLASH] Read combination statistics:
      //[FLASH]     Total reads:      334434
      //[FLASH]     Combined reads:   984
      //[FLASH]     Uncombined reads: 333450
      val totalRe = """\Q[FLASH]\E\s+\QTotal pairs:\E\s+(\d+)\s+""".r
      val combinedRe = """\Q[FLASH]\E\s+\QCombined pairs:\E\s+(\d+)\s+""".r
      val uncombinedRe = """\Q[FLASH]\E\s+\QUncombined pairs:\E\s+(\d+)\s+""".r

      val readsStats = new ReadStatsBuilder()

      flashOut.split("\n").foreach {
        case totalRe(n) => readsStats.total = n.toLong
        case combinedRe(n) => readsStats.merged = n.toLong
        case uncombinedRe(n) => readsStats.notMerged = n.toLong
        case s => () //logger.info("unparsed: " + s)
      }

      val mergedFile = new File(workingDirectory, "out.extendedFrags.fastq")
      val notMerged1 = Some(new File(workingDirectory, "out.notCombined_1.fastq"))
      val notMerged2 = Some(new File(workingDirectory, "out.notCombined_2.fastq"))

      MergeToolResult(mergedFile, notMerged1, notMerged2, readsStats.build)
    }
  }
}

object FLAShMergingTool {
  val defaultTemplate: List[String] = List("$file1$", "$file2$")

  def linux(logger: Logger, workingDirectory: File, loadingManager: LoadingManager, flashTemplate: List[String]): Try[FLAShMergingTool] = {
    val flashAddress = ObjectAddress("metapasta", "flash-1.2.11")

    Try {
      val flash = "flash"
      val flashDst = new File(workingDirectory, flash)
      loadingManager.download(flashAddress, flashDst)
      flashDst.setExecutable(true)
      new FLAShMergingTool(flashDst, flashTemplate)
    }.recoverWith { case t =>
      Failure(new Error("couldn't install FLASh from " + flashAddress, t))
    }
  }

  def windows(logger: Logger, workingDirectory: File, loadingManager: LoadingManager, flashTemplate: List[String]): Try[FLAShMergingTool] = {
    val flash = "flash.exe"
    val flashAddress = ObjectAddress("metapasta", "flash-1.2.11.exe")
    Try {

      val flashDst = new File(workingDirectory, flash)
      if (!flashDst.exists()) {
        loadingManager.download(flashAddress, flashDst)
        flashDst.setExecutable(true)
      }
      new FLAShMergingTool(flashDst, flashTemplate)
    }.recoverWith { case t =>
      Failure(new Error("couldn't install FLASh from " + flashAddress, t))
    }
  }
}