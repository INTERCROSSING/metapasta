package ohnosequences.metapasta

import ohnosequences.awstools.s3.{S3, ObjectAddress}
import ohnosequences.nisperon.{Instructions, AWS, MapInstructions}
import org.clapper.avsl.Logger
import java.io.File
import ohnosequences.formats.{RawHeader, FASTQ}
import ohnosequences.nisperon.logging.S3Logger


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

case class MergedSampleChunk(fastq: ObjectAddress, sample: String, range: (Long, Long)) {
  def chunkId = range._1 + "-" + sample
}

//case class ParsedSampleChunk(name: String, fastqs: List[FASTQ[RawHeader]]) {
//  def toFasta: String = {
//    fastqs.map(_.toFasta).mkString("\r")
//  }
//
//  def toFastq: String = {
//    fastqs.map(_.toFastq).mkString("\r")
//  }
//}

class FlashInstructions(aws: AWS, bucket: String, chunkSize: Int = 2000000) extends Instructions[List[PairedSample], List[MergedSampleChunk]] {

  import scala.sys.process._

  val logger = Logger(this.getClass)

  val lm = aws.s3.createLoadingManager()

  override def prepare() {

    logger.info("creating bucket " + bucket)
    aws.s3.createBucket(bucket)

    val flash = "flash"
    val flashDst = new File("/usr/bin", flash)
    lm.download(ObjectAddress("metapasta", flash), flashDst)
    flashDst.setExecutable(true)

  }

  def solve(input: List[PairedSample], s3logger: S3Logger): List[List[MergedSampleChunk]] = {
    val sample = input.head

    val resultObject = if (sample.fastq1.equals(sample.fastq2)) {
      logger.info("not paired-ended")
      sample.fastq1
    } else {
      logger.info("downloading " + sample.fastq1)
      lm.download(sample.fastq1, new File("1.fastq"))

      logger.info("downloading " + sample.fastq2)
      lm.download(sample.fastq2, new File("2.fastq"))

      logger.info("executing FLASh")
      "flash 1.fastq 2.fastq".!

      logger.info("uploading results")
      val resultObject2 = ObjectAddress(bucket, "merged/" + sample.name + ".fastq")
      lm.upload(resultObject2, new File("out.extendedFrags.fastq"))
      resultObject2
    }


    val ranges = new S3Splitter(aws.s3, resultObject, chunkSize).chunks()

    //todo remove limit
    ranges.map { range =>
      List(MergedSampleChunk(resultObject, sample.name, range))
    }
  }
}
