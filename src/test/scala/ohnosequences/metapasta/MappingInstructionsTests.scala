package ohnosequences.metapasta

import java.io.File

import ohnosequences.compota.environment.Env
import ohnosequences.logging.Logger
import ohnosequences.metapasta.databases.{BlastDatabase, GI}
import ohnosequences.metapasta.instructions.Blast
import org.junit.Assert._
import org.junit.Test


class MappingInstructionsTests extends MetapastaTest {

 // @Test
  def mappingTool(): Unit = {

    val testsWorkingDirectory = new File("test")
    testsWorkingDirectory.mkdir()
    val workingDirectory2 = new File(testsWorkingDirectory, "mapping")
    workingDirectory2.mkdir()

    launch("mappingTool", true) { case (logger2, aws, loadingManager) =>
      val env = new Env {
        override val logger: Logger = logger2

        override def isStopped: Boolean = false

        override val workingDirectory: File = workingDirectory2
      }

      val databaseDirectory = new File(testsWorkingDirectory, "blastDatabase")
      BlastDatabase.march2014database.get(logger2, databaseDirectory, loadingManager).flatMap { database =>
        Blast.windows[GI](Blast.defaultBlastnTemplate).get(logger2, workingDirectory2, loadingManager).map { blast =>
          val testReads = s3location / "supermock2.fasta"
          logger2.info("downloading test reads from " + testReads)
          val testReadsFile = new File(workingDirectory2, "supermock2.fasta")
          if (!testReadsFile.exists()) {
            loadingManager.download(testReads, testReadsFile)
          }
          val blastOutput = new File(workingDirectory2, "blast.out")
          blast.launch(logger2, workingDirectory2, database, testReadsFile, blastOutput).map { hits =>
            assertEquals("hits length", 3, hits.size)
            assertEquals("has read 1", true, hits.exists { hit => hit.readId.readId.equals("ERR341325.01") && hit.refId.id.equals("311690795") })
            assertEquals("has read 2", true, hits.exists { hit => hit.readId.readId.equals("ERR341325.02") && hit.refId.id.equals("379334228") })
            assertEquals("has read 3", true, hits.exists { hit => hit.readId.readId.equals("ERR341325.03") && hit.refId.id.equals("401836662") })
          }
        }
      }
    }
  }


//  @Test
  def blastDatabase(): Unit = {
    launch("blastDatabase", false) { case (logger, aws, loadingManager) =>
      val testsWorkingDirectory = new File("test")
      testsWorkingDirectory.mkdir()
      val workingDirectory = new File(testsWorkingDirectory, "blastDatabase")
      workingDirectory.mkdir()
      BlastDatabase.march2014database.get(logger, workingDirectory, loadingManager)
    }
  }
}
