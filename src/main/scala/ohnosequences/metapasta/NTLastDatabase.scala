package ohnosequences.metapasta

import ohnosequences.nisperon.AWS
import org.clapper.avsl.Logger
import ohnosequences.awstools.s3.ObjectAddress
import java.io.File

object NTLastDatabase extends LastDatabase {

  val logger = Logger(this.getClass)

  def install(aws: AWS) {

    logger.info("downloading database")
    val lm = aws.s3.createLoadingManager()

    lm.downloadDirectory(ObjectAddress("metapasta", "nt.last"), new File("."))

  }

  val name: String = "nt"

  //gi|313494140|gb|GU939576.1|
  val re = """\Qgi|\E(\d+)[^\d]+.*""".r
  def parseGI(refId: String) : String = {
    refId match {
      case re(id) => id
      case s => throw new Error("couldn't extract GI from ref id: " + s)
    }
  }
}
