package ohnosequences.metapasta.instructions

import ohnosequences.awstools.s3.{S3, ObjectAddress}
import ohnosequences.nisperon.{MapMonoid, Instructions, AWS, MapInstructions}
import org.clapper.avsl.Logger
import java.io.File
import ohnosequences.formats.FASTQ
import ohnosequences.nisperon.logging.S3Logger
import ohnosequences.metapasta._
import ohnosequences.metapasta.MergedSampleChunk
import ohnosequences.metapasta.PairedSample
import ohnosequences.awstools.s3.ObjectAddress
import ohnosequences.metapasta.ReadsStats


class FlashInstructions(aws: AWS, bucket: String, chunkSize: Int = 2000000) extends Instructions[List[PairedSample], (Map[(String, String), ReadsStats], List[MergedSampleChunk])] {

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

//  //todo do it more precise
//  def countUnmerged(): Int = {
//    var count = 0
//    try {
//      io.Source.fromFile("out.notCombined_1.fastq").getLines().foreach {
//        str => count += 1
//      }
//    } catch {
//      case t: Throwable => ()
//    }
//    try {
//      io.Source.fromFile("out.notCombined_2.fastq").getLines().foreach {str => count += 1}
//    } catch {
//      case t: Throwable => ()
//    }
//    count / 4
//    //out.notCombined_2.fastq
//  }

  //todo do it more precise
  def countReads(file: File): Long = {
    var count = 0L
    try {
      io.Source.fromFile(file).getLines().foreach {
        str => count += 1
      }
    } catch {
      case t: Throwable => ()
    }
    count / 4
  }

  //(String, String), ReadsStats] (sample,assignmentType) -> readsStat
  def solve(input: List[PairedSample], s3logger: S3Logger, context: Context): List[(Map[(String, String), ReadsStats], List[MergedSampleChunk])] = {
    import sys.process._

    val sample = input.head

    val statsBuilder = new ReadsStatsBuilder()
    val (resultObject, stats) = if (sample.fastq1.equals(sample.fastq2)) {
      logger.info("not paired-ended")

      if(sample.fastq1.key.endsWith(".gz")) {
        logger.info("downloading " + sample.fastq1)
        lm.download(sample.fastq1, new File("1.fastq.gz"))

        logger.info("extracting")
        "gunzip 1.fastq.gz".!

        val extracted = new File("1.fastq")

        logger.info("counting reads")
        val reads = countReads(extracted)

        statsBuilder.total = reads
        statsBuilder.merged = reads

        logger.info("uploading results")
        val resultObject2 = ObjectAddress(bucket, "merged/" + sample.name + ".fastq")
        lm.upload(resultObject2, extracted)


        (resultObject2, statsBuilder.build)
      } else {
        logger.info("downloading " + sample.fastq1)
        val fastq =  new File("1.fastq")
        lm.download(sample.fastq1, fastq)

        logger.info("counting reads")
        val reads = countReads(fastq)
        statsBuilder.total = reads
        statsBuilder.merged = reads

        (sample.fastq1, statsBuilder.build)
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
        lm.download(sample.fastq2, new File("2.fastq.gz"))
        logger.info("extracting")
        "gunzip 2.fastq.gz".!
      } else {
        lm.download(sample.fastq2, new File("2.fastq"))
      }


      logger.info("executing FLASh")
      val flashOut = "flash 1.fastq 2.fastq".!!

      //[FLASH] Read combination statistics:
      //[FLASH]     Total reads:      334434
      //[FLASH]     Combined reads:   984
      //[FLASH]     Uncombined reads: 333450
      val totalRe = """\Q[FLASH]\E\s+\QTotal reads:\E\s+(\d+)""".r
      val combinedRe = """\Q[FLASH]\E\s+\QCombined reads:\E\s+(\d+)""".r
      val uncombinedRe = """\Q[FLASH]\E\s+\QUncombined reads:\E\s+(\d+)""".r

      val readsStats = new ReadsStatsBuilder()

      flashOut.split("\n").foreach {
        case totalRe(n) => readsStats.total = n.toLong
        case combinedRe(n) => readsStats.merged = n.toLong
        case uncombinedRe(n) => readsStats.notMerged = n.toLong
        case _ =>
      }

      logger.info("uploading results")
      val resultObject2 = ObjectAddress(bucket, "merged/" + sample.name + ".fastq")
      lm.upload(resultObject2, new File("out.extendedFrags.fastq"))
      (resultObject2, readsStats.build)
    }


    val ranges = new S3Splitter(aws.s3, resultObject, chunkSize).chunks()

    //todo remove limit

    val mapMonoid = new MapMonoid[(String, String), ReadsStats](readsStatsMonoid)

    var first = true
    ranges.map { range =>

      val stats2  = if (first) {
        first = false
        Map((sample.name, AssignmentType.BBH) -> stats, (sample.name, AssignmentType.LCA) -> stats)
      } else {
        mapMonoid.unit
      }


      (stats2, List(MergedSampleChunk(resultObject, sample.name, range)))
    }

  }
}
