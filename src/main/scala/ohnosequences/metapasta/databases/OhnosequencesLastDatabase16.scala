package ohnosequences.metapasta.databases

import ohnosequences.logging
import ohnosequences.awstools.s3.{LoadingManager, ObjectAddress}

import java.io.File

import scala.util.Try

class OhnosequencesLastDatabase16(
                       s3location: ObjectAddress = ObjectAddress("metapasta", "nt.march.14.last")
                       ) extends Installable[LastDatabase16S[GI]] {


  class Database(lastDirectory: File) extends LastDatabase16S[GI] {
    
    val name: String = "nt.march.14.last/nt.march.14"

    override def location: File = lastDirectory

    //gi|313494140|gb|GU939576.1|
    val re = """\Qgi|\E(\d+)[^\d]+.*""".r

    override def parseRawId(refId: String): Option[GI] = {
      refId match {
        case re(id) => Some(GI(id))
        case s => None
      }
    }
  }


  override protected def install(logger: logging.Logger, workingDirectory: File, loadingManager: LoadingManager): Try[LastDatabase16S[GI]] = {
    Try {
      logger.info("downloading LAST database from " + s3location.url)
      val lastDirectory = new File(workingDirectory, "last-database")
      loadingManager.downloadDirectory(s3location, lastDirectory)
      new Database(lastDirectory)
    }
  }
}
