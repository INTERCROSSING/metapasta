package ohnosequences.metapasta

import ohnosequences.awstools.s3.ObjectAddress
import ohnosequences.nisperon.{AWS, MapInstructions}
import org.clapper.avsl.Logger
import java.io.File


case class PairedSample(fastq1: ObjectAddress, fastq2: ObjectAddress)
case class Sample(fastq: ObjectAddress)

class FlashInstructions(aws: AWS) extends MapInstructions[List[PairedSample], List[Sample]] {

  val logger = Logger(this.getClass)

  val lm = aws.s3.createLoadingManager()

  override def prepare() {
    import scala.sys.process._

    logger.info("downloading FLASH")
    val flash = "FLASH-1.2.8.tar.gz"
    lm.download(ObjectAddress("metapasta", flash), new File(flash))

    logger.info("extracting FLASH")
    ("tar xzf " + flash).!

    logger.info("installing gcc")
    "yum install gcc -y".!

    logger.info("installing zlib-devel")
    "yum install zlib-devel -y".!

    logger.info("building FLASH")
    Process(Seq("make"), new java.io.File(flash.replace(".tar.gz", ""))).!


  }

  def apply(input: List[PairedSample]) = List()
}
