package ohnosequences.metapasta

import ohnosequences.nisperon.AWS
import java.io.File
import org.clapper.avsl.Logger
import ohnosequences.awstools.s3.ObjectAddress


object NTDatabase extends BlastDatabase {

  val logger = Logger(this.getClass)

  def install(aws: AWS) {
    import scala.sys.process._

    logger.info("downloading database")
    val lm = aws.s3.createLoadingManager()
    lm.download(ObjectAddress("metapasta", "nt.blast.zip"), new File("database.zip"))

    logger.info("extracting database")
    """unzip database.zip""".!

  }

  val name: String = "nt.16S.fasta"

  //gi|313494140|gb|GU939576.1|
  val re = """\Qgi|\E(\d+)[^\d]+.*""".r
  def parseGI(refId: String) : String = {
    refId match {
      case re(id) => id
      case s => throw new Error("couldn't extract GI from ref id: " + s)
    }
  }
}
