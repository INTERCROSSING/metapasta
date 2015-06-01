package ohnosequences.metapasta

import java.io.File

import ohnosequences.awstools.s3.{LoadingManager, ObjectAddress}
import ohnosequences.compota.aws.{MetapastaTestCredentials}
import ohnosequences.logging.{Logger, ConsoleLogger}
import ohnosequences.metapasta.instructions.FLAShMergingTool
import org.junit.Assert._
import org.junit.Test

import scala.util.{Success, Try, Failure}

/**
 * Created by Evdokim on 01.06.2015.
 */

trait MetapastaTest {
  val s3location: ObjectAddress = ObjectAddress("metapasta", "test")

  def launch[T](name: String, debug: Boolean = false)(action: (Logger, LoadingManager) => Try[T]): Unit = {
    val logger = new ConsoleLogger(name, debug)
    MetapastaTestCredentials.loadingManager match {
      case None => logger.warn("aws credentials should be defined for this test")
      case Some(loadingManager) => {
        action(logger, loadingManager) match {
          case Failure(t) => {
            logger.error(t)
            fail(t.toString)
          }
          case Success(r) => () //logger.info(r.toString)
        }
      }
    }
  }
}

class MergeInstructionsTests extends MetapastaTest {

  @Test
  def flashTool(): Unit = {
    launch("flashTool", false) { case (logger, loadingManager) =>

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


}
