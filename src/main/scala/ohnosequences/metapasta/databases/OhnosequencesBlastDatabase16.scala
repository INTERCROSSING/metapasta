package ohnosequences.metapasta.databases

import ohnosequences.awstools.s3.{LoadingManager, ObjectAddress}
import java.io.File

import ohnosequences.logging.Logger

import scala.util.Try

class OhnosequencesBlastDatabase16(
  s3Location: ObjectAddress = ObjectAddress("metapasta", "nt.march.14.blast")
) extends Installable[BlastDatabase16S[GI]] {


  class Database(databaseDirectory: File) extends BlastDatabase16S[GI] {

    val name: String = databaseDirectory.getAbsolutePath

    override def location: File = databaseDirectory

    //gi|313494140|gb|GU939576.1|
    val re = """\Qgi|\E(\d+)[^\d]+.*""".r
    override def parseRawId(rawReferenceId: String): Option[GI] = rawReferenceId match {
      case re(id) => Some(GI(id))
      case s => None
    }

  }


  override protected def install(logger: Logger, workingDirectory: File, loadingManager: LoadingManager): Try[BlastDatabase16S[GI]] = {
    Try {
      logger.info("downloading BLAST database from " + s3Location)
      val databaseDirectory = new File(workingDirectory, "blast-database")
      loadingManager.downloadDirectory(s3Location, databaseDirectory)
      new Database(databaseDirectory)
    }
  }


}
