package ohnosequences.metapasta.databases

import ohnosequences.logging.Logger
import ohnosequences.metapasta.Factory
import ohnosequences.awstools.s3.{LoadingManager, ObjectAddress}
import java.io.File

import scala.util.Try

object blast16SFactory extends DatabaseFactory[BlastDatabase16S[GI]] {


  class BlastDatabase extends BlastDatabase16S[GI] {
    val name: String = "nt.march.14.blast/nt.march.14.fasta"

    //gi|313494140|gb|GU939576.1|
    val re = """\Qgi|\E(\d+)[^\d]+.*""".r


    override def parseRawId(refId: String): Option[GI] = {
      refId match {
        case re(id) => Some(GI(id))
        case s => None
      }
    }
  }

  override def build(logger: Logger, loadingManager: LoadingManager): Try[BlastDatabase] = {
    Try {
      logger.info("downloading database")
      loadingManager.downloadDirectory(ObjectAddress("metapasta", "nt.march.14.blast"), new File("."))
      new BlastDatabase()
    }
  }
}
