package ohnosequences.metapasta.databases

import ohnosequences.logging.Logger
import ohnosequences.awstools.s3.{LoadingManager, ObjectAddress}
import java.io.File

import scala.util.Try


class BlastDatabase(val path: File, val baseName: String) extends BlastDatabase16S[GI] {
  //val name: String = "nt.march.14.blast/nt.march.14.fasta"

  def name: String = new File(path, baseName).getAbsolutePath

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
  def era7database(logger: Logger, workingDirectory: File, loadingManager: LoadingManager, name: String, s3location: ObjectAddress): Try[BlastDatabase] = {
    Try {
      logger.info("downloading 16S database from " + s3location)
      val blastDatabase = new File(workingDirectory, "blastDatabase")
      loadingManager.downloadDirectory(s3location, blastDatabase)
      new BlastDatabase(blastDatabase, name)
    }
  }
}
