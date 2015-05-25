package ohnosequences.metapasta.instructions

import java.io.File
import java.util.logging.Logger

import ohnosequences.metapasta.{S3Paths, ReadStatsBuilder}

import scala.util.Try

case class MergeStat(total: Long = 0, merged: Long = 0, notMerged: Long = 0)

case class MergeToolResult (
  merged: File,
  notMerged1: File,
  notMerged2: File

)

abstract class MergingTool {
  def merge(logger: Logger, workingDirectory: File, file1: File, file2: File): Try[(MergeStat, File)]
}


class FLAShMergingTool(flashTemplate: List[String]) extends MergingTool {

  import sys.process._

  override def merge(logger: Logger, workingDirectory: File, reads1: File, reads2: File): Try[(MergeStat, File)] = {
    val flashCommand = flashTemplate

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

    logger.info("uploading results")
    val resultObject2 = S3Paths.mergedFastq(readsDirectory, sample.name)

    lm.upload(resultObject2, new File("out.extendedFrags.fastq"))

    val file1 = new File("out.notCombined_1.fastq")
    val file2 = new File("out.notCombined_1.fastq")
    val dest = S3Paths.notMergedFastq(readsDirectory, sample.name)
    logger.info("uploading not merged file " + file1 + " to " + dest._1)
    lm.upload(dest._1, file1)
    logger.info("uploading not merged file " + file2 + " to " + dest._2)
    lm.upload(dest._2, file2)

    (resultObject2, readsStats.build)
  }
}