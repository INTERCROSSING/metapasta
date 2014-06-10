package ohnosequences.metapasta.instructions

import ohnosequences.awstools.s3.{S3, ObjectAddress}
import ohnosequences.nisperon.{Instructions, AWS, MapInstructions}
import org.clapper.avsl.Logger
import java.io.File
import ohnosequences.formats.FASTQ
import ohnosequences.nisperon.logging.S3Logger
import ohnosequences.metapasta._
import ohnosequences.metapasta.MergedSampleChunk
import ohnosequences.metapasta.PairedSample
import ohnosequences.awstools.s3.ObjectAddress
import ohnosequences.metapasta.ReadsStats


class FlashInstructions(aws: AWS, bucket: String, chunkSize: Int = 2000000) extends Instructions[List[PairedSample], (ReadsStats, List[MergedSampleChunk])] {

  import scala.sys.process._

  val logger = Logger(this.getClass)

  val lm = aws.s3.createLoadingManager()

  override type Context = Unit

  override def prepare() {

    logger.info("creating bucket " + bucket)
    aws.s3.createBucket(bucket)

    val flash = "flash"
    val flashDst = new File("/usr/bin", flash)
    lm.download(ObjectAddress("metapasta", flash), flashDst)
    flashDst.setExecutable(true)

  }

  //todo do it more precise
  def countUnmerged(): Int = {
    var count = 0
    try {
      io.Source.fromFile("out.notCombined_1.fastq").getLines().foreach {
        str => count += 1
      }
    } catch {
      case t: Throwable => ()
    }
    try {
      io.Source.fromFile("out.notCombined_2.fastq").getLines().foreach {str => count += 1}
    } catch {
      case t: Throwable => ()
    }
    count / 4 
    //out.notCombined_2.fastq
  }

  def solve(input: List[PairedSample], s3logger: S3Logger, context: Context): List[(ReadsStats, List[MergedSampleChunk])] = {
    import sys.process._

    val sample = input.head

    val resultObject = if (sample.fastq1.equals(sample.fastq2)) {
      logger.info("not paired-ended")
      if(sample.fastq1.key.endsWith(".gz")) {
        logger.info("downloading " + sample.fastq1)
        lm.download(sample.fastq1, new File("1.fastq.gz"))

        "gunzip 1.fastq.gz".!

        logger.info("uploading results")
        val resultObject2 = ObjectAddress(bucket, "merged/" + sample.name + ".fastq")
        lm.upload(resultObject2, new File("1.fastq"))
        resultObject2
      } else {
        sample.fastq1
      }
    } else {
      logger.info("downloading " + sample.fastq1)
      if(sample.fastq1.key.endsWith(".gz")) {
        lm.download(sample.fastq1, new File("1.fastq.gz"))
        logger.info("extracting")
        "gunzip 1.fastq.gz".!
      } else {
        lm.download(sample.fastq1, new File("1.fastq"))
      }

      if(sample.fastq2.key.endsWith(".gz")) {
        lm.download(sample.fastq1, new File("2.fastq.gz"))
        logger.info("extracting")
        "gunzip 2.fastq.gz".!
      } else {
        lm.download(sample.fastq1, new File("2.fastq"))
      }


      logger.info("executing FLASh")
      "flash 1.fastq 2.fastq".!

      logger.info("uploading results")
      val resultObject2 = ObjectAddress(bucket, "merged/" + sample.name + ".fastq")
      lm.upload(resultObject2, new File("out.extendedFrags.fastq"))
      resultObject2
    }


    val ranges = new S3Splitter(aws.s3, resultObject, chunkSize).chunks()

    //todo remove limit

    var first = true
    ranges.map { range =>

      val stats  = if (first) {
        first = false
        ReadsStats(unmerged = countUnmerged())
      } else {
        readsStatsMonoid.unit
      }
      (stats, List(MergedSampleChunk(resultObject, sample.name, range)))
    }

  }
}
