package ohnosequences.metapasta.databases

import java.io.File

import ohnosequences.awstools.s3.{LoadingManager, ObjectAddress}
import ohnosequences.logging.Logger

import scala.util.Try


class GIBlastDatabase(val location: File, val name: String) extends BlastDatabase16S[GI] {
  //val name: String = "nt.march.14.blast/nt.march.14.fasta"


  //gi|313494140|gb|GU939576.1|
  val re = """\Qgi|\E(\d+)[^\d]+.*""".r


  override def parseRawId(refId: String): Option[GI] = {
    refId match {
      case re(id) => Some(GI(id))
      case s => None
    }
  }
}


object BlastDatabase {

  object march2014database extends Installable[BlastDatabase16S[GI]] {
    override def install(logger: Logger, workingDirectory: File, loadingManager: LoadingManager): Try[BlastDatabase16S[GI]] = {
      era7database(logger, workingDirectory, loadingManager, "nt.march.14.fasta", ObjectAddress("metapasta", "nt.march.14.blast"))
    }
  }


  def era7database(logger: Logger, workingDirectory: File, loadingManager: LoadingManager, name: String, s3location: ObjectAddress): Try[BlastDatabase16S[GI]] = {
    Try {
      logger.info("downloading 16S database from " + s3location)
      val blastDatabase = new File(workingDirectory, "nt.march.14.blast")
      if (!blastDatabase.exists()) {
        blastDatabase.mkdir()
        loadingManager.downloadDirectory(s3location, workingDirectory)
      }
      new GIBlastDatabase(blastDatabase, name)
    }
  }
}
