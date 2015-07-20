package ohnosequences.metapasta

import java.io.File

import ohnosequences.awstools.AWSClients
import ohnosequences.awstools.s3.{LoadingManager, ObjectAddress}
import ohnosequences.compota.aws.MetapastaTestCredentials
import ohnosequences.compota.environment.Env
import ohnosequences.logging.{ConsoleLogger, Logger}
import ohnosequences.metapasta.instructions.{FLAShMergingTool, MergingInstructions}
import org.junit.Assert._
import org.junit.Test

import scala.util.{Failure, Success, Try}



class MergeInstructionsTests extends MetapastaTest {

 // @Test
  def flashTool(): Unit = {
    launch("flashTool", false) { case (logger, aws, loadingManager) =>

      val testsWorkingDirectory = new File("test")
      testsWorkingDirectory.mkdir()
      val mergeInstructionsTest = new File(testsWorkingDirectory, "merge")
      mergeInstructionsTest.mkdir()

      val flashInstall = if (System.getProperty("os.name").startsWith("Windows")) {
        logger.info("installing FLASh for Windows")
        FLAShMergingTool.windows(logger, mergeInstructionsTest, loadingManager, FLAShMergingTool.defaultTemplate)
      } else {
        logger.info("installing FLASh for Linux")
        FLAShMergingTool.linux(logger, mergeInstructionsTest, loadingManager, FLAShMergingTool.defaultTemplate)
      }
      flashInstall.flatMap { flashTool =>
        val file1 = new File(mergeInstructionsTest, "file1.fastq")
        if (!file1.exists()) {
          loadingManager.download(s3location / "test1s.fastq", file1)
        }
        val file2 = new File(mergeInstructionsTest, "file2.fastq")
        if (!file2.exists()) {
          loadingManager.download(s3location / "test2s.fastq", file2)
        }
        flashTool.merge(logger, mergeInstructionsTest, file1, file2).map { mergeResult =>
          assertEquals("merged file exists", true, mergeResult.merged.exists())
          assertEquals("counts", 905, mergeResult.stats.merged)
          assertEquals("counts", 95, mergeResult.stats.notMerged)
          assertEquals("counts", 1000, mergeResult.stats.total)
        }
      }
    }
  }

//  @Test
  def mergingInstructions(): Unit = {

    val testsWorkingDirectory = new File("test")
    testsWorkingDirectory.mkdir()

    val mergeInstructionsTest = new File(testsWorkingDirectory, "merge")
    mergeInstructionsTest.mkdir()

    launch("mergingInstructions", false) { case (logger2, aws, loadingManager) =>
      val env = new Env {

        override val logger: Logger = logger2

        override def isStopped: Boolean = false

        override val workingDirectory: File = mergeInstructionsTest
      }

      val mergingConfiguration = MergingInstructionsConfiguration(
        loadingManager = { l => Success(loadingManager) },
        mergingTool = {
          case (logger, workingDirectory, loadingManager) =>
            if (isWindows) {
              FLAShMergingTool.windows(logger, workingDirectory, loadingManager, FLAShMergingTool.defaultTemplate)
            } else {
              FLAShMergingTool.linux(logger, workingDirectory, loadingManager, FLAShMergingTool.defaultTemplate)
            }
        },
        mergedReadsDestination = { s => s3location / "merged.fasta" },
        notMergedReadsDestination = { s => (s3location / "notMerged1.fasta", s3location / "notMerged2.fasta") },
        Some(10),
        10000
      )
      val mergingInstructions = new MergingInstructions(mergingConfiguration)
      mergingInstructions.prepare(env).flatMap { context =>
        val sample = PairedSample("testSample", s3location / "test1s.fastq", s3location / "test2s.fastq.gz")
        val input = List(sample)
        mergingInstructions.solve(env, context, input).map { res =>
          assertEquals("results not empty", false, res.isEmpty)

          val stats1 = res.head._2((sample.name, BBH))
          val stats2 = res.head._2((sample.name, LCA))
          assertEquals("BBH and LCA stats are the same", true, stats1.equals(stats2))


          assertEquals("merged object exists", true, aws.s3.objectExists(res.head._1.head.fastq).get)
          assertEquals("counts", 905, stats1.merged)
          assertEquals("counts", 95, stats1.notMerged)
          assertEquals("counts", 1000, stats1.total)
        }
      }

    }
  }


}
