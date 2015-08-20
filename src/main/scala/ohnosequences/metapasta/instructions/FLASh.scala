package ohnosequences.metapasta.instructions

import ohnosequences.awstools.s3.{ObjectAddress, LoadingManager}
import ohnosequences.logging.Logger
import ohnosequences.metapasta.ReadStatsBuilder
import ohnosequences.metapasta.databases.Installable

import scala.util.{Failure, Try}
import scala.util.matching.Regex

import java.io.File

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



class Era7FLASh(commandTemplate: List[String] = List("$file1$", "$file2$"), s3location: ObjectAddress = ObjectAddress("metapasta", "flash/1.2.11/flash")) extends Installable[MergingTool] {

  override protected def install(logger: Logger, workingDirectory: File, loadingManager: LoadingManager): Try[FLAShMergingTool] = {
    Try {
      val flash = "flash"
      val flashDst = new File(workingDirectory, flash)
      loadingManager.download(s3location, flashDst)
      flashDst.setExecutable(true)
      new FLAShMergingTool(flashDst, commandTemplate)
    }.recoverWith { case t =>
      Failure(new Error("couldn't install FLASh from " + s3location, t))
    }
  }

}
