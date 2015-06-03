package ohnosequences.metapasta

import java.io.File

import ohnosequences.compota.environment.Env
import ohnosequences.logging.Logger
import ohnosequences.metapasta.databases.{BlastDatabase16S, GI, BlastDatabase}
import ohnosequences.metapasta.instructions.Blast
import org.junit.Test
import org.junit.Assert._


class MappingInstructionsTests extends MetapastaTest {

    @Test
    def mappingTool(): Unit = {

      val testsWorkingDirectory = new File("test")
      testsWorkingDirectory.mkdir()
      val workingDirectory = new File(testsWorkingDirectory, "mapping")
      workingDirectory.mkdir()

      launch("mappingTool", true) { case (logger2, aws, loadingManager) =>
        val env = new Env {
          override val logger: Logger = logger2
          override def isStopped: Boolean = false
          override val workingDirectory: File = workingDirectory
        }

        BlastDatabase.march2014database.get(logger2, workingDirectory, loadingManager).flatMap { database =>
          Blast.windows[GI](Blast.defaultBlastnTemplate).get(logger2, workingDirectory, loadingManager).map { blast =>
            val testReads = s3location / "supermock2.fasta"
            logger2.info("donloading test reads from " + testReads)
            val testReadsFile = new File(workingDirectory, "supermock2.fasta")
            if (!testReadsFile.exists()) {
              loadingManager.download(testReads, testReadsFile)
            }
            val blastOutput = new File(workingDirectory, "blast.out")
            blast.launch(logger2, workingDirectory, database, testReadsFile, blastOutput).map { hits =>
              assertEquals("hits lenght", 3, hits.size)
              assertEquals("has read 1", true, hits.exists { hit => hit.readId.readId.equals("ERR341325.01") && hit.refId.id.equals("311690795")})
              assertEquals("has read 2", true, hits.exists { hit => hit.readId.readId.equals("ERR341325.02") && hit.refId.id.equals("379334228")})
              assertEquals("has read 3", true, hits.exists { hit => hit.readId.readId.equals("ERR341325.03") && hit.refId.id.equals("401836662")})
            }
          }
        }
      }
    }


  @Test
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
