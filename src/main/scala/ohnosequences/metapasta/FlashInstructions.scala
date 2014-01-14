package ohnosequences.metapasta

import ohnosequences.awstools.s3.{S3, ObjectAddress}
import ohnosequences.nisperon.{SplitInstructions, AWS, MapInstructions}
import org.clapper.avsl.Logger
import java.io.File
import ohnosequences.formats.{RawHeader, FASTQ}


case class PairedSample(name: String, fastq1: ObjectAddress, fastq2: ObjectAddress)

//case class Read(header: String, sequence: String) {
//  def toFasta: String = {
//    ">" + header + "\r" + sequence
//  }
//}



class S3Splitter(s3: S3, address: ObjectAddress, chunksSize: Long) {

  def objectSize(): Long = {
    s3.s3.getObjectMetadata(address.bucket, address.key).getContentLength
  }

  def chunks(): List[(Long, Long)] = {

    val size = objectSize()

    val starts = 0L until size by chunksSize
    val ends = starts.map { start =>
      math.min(start + chunksSize - 1, size - 1)
    }

    starts.zip(ends).toList
  }

//  def chunksAmount(): Long = {
//    val objSize = objectSize()
//    objSize / chunksSize + { if (objSize % chunksSize == 0) 0 else 1 }
//  }
}

case class ProcessedSampleChunk(fastq: ObjectAddress, name: String, range: (Long, Long))

case class ParsedSampleChunk(name: String, fastqs: List[FASTQ[RawHeader]]) {
  def toFasta: String = {
    fastqs.map(_.toFasta).mkString("\r")
  }

  def toFastq: String = {
    fastqs.map(_.toFastq).mkString("\r")
  }
}

class FlashInstructions(aws: AWS, bucket: String) extends SplitInstructions[List[PairedSample], List[ProcessedSampleChunk]] {

  import scala.sys.process._

  val logger = Logger(this.getClass)

  val lm = aws.s3.createLoadingManager()

  override def prepare() {

//
//    old installing with building
//
//    logger.info("downloading FLASH")
//    val flash = "FLASH-1.2.8.tar.gz"
//    lm.download(ObjectAddress("metapasta", flash), new File(flash))
//
//    logger.info("extracting FLASH")
//    ("tar xzf " + flash).!
//
//    logger.info("installing gcc")
//    "yum install gcc -y".!
//
//    logger.info("installing zlib-devel")
//    "yum install zlib-devel -y".!
//
//    logger.info("building FLASH")
//    Process(Seq("make"), new java.io.File(flash.replace(".tar.gz", ""))).!

    logger.info("creating bucket " + bucket)
    aws.s3.createBucket(bucket)

    val flash = "flash"
    val flashDst = new File("/usr/bin", flash)
    lm.download(ObjectAddress("metapasta", flash), flashDst)
    flashDst.setExecutable(true)


  }

  def apply(input: List[PairedSample]): List[List[ProcessedSampleChunk]] = {
    val sample = input.head


    logger.info("downloading " + sample.fastq1)
    lm.download(sample.fastq1, new File("1.fastq"))

    logger.info("downloading " + sample.fastq2)
    lm.download(sample.fastq2, new File("2.fastq"))


    logger.info("executing FLASh")
    "flash 1.fastq 2.fastq".!

    logger.info("uploading results")
    val resultObject = ObjectAddress(bucket, "merged/" + sample.name + ".fastq")
    lm.upload(resultObject, new File("out.extendedFrags.fastq"))


    val ranges = new S3Splitter(aws.s3, resultObject, 10000).chunks()

    ranges.map { range =>
      List(ProcessedSampleChunk(resultObject, sample.name, range))
    }
  }
}
