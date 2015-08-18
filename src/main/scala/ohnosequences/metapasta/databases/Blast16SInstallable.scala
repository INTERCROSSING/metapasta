package ohnosequences.metapasta.databases

import ohnosequences.awstools.s3.{LoadingManager, ObjectAddress}
import java.io.File

import ohnosequences.logging.Logger

import scala.util.Try

object Blast16SInstallable extends Installable[BlastDatabase16S] {


  class BlastDatabase(workingDirectory: File) extends BlastDatabase16S {
    val name: String = "nt.march.14.blast/nt.march.14.fasta"

    //gi|313494140|gb|GU939576.1|
    val re = """\Qgi|\E(\d+)[^\d]+.*""".r
    def parseGI(refId: String) : Option[String] = {
      refId match {
        case re(id) => Some(id)
        case s => None
      }
    }
  }


  override protected def install(logger: Logger, workingDirectory: File, loadingManager: LoadingManager): Try[BlastDatabase16S] = {
    Try {
      logger.info("downloading database")
      loadingManager.downloadDirectory(ObjectAddress("metapasta", "nt.march.14.blast"), new File("."))
      new BlastDatabase(workingDirectory)
    }
  }


}
