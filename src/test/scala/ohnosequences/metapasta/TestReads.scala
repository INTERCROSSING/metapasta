package ohnosequences.metapasta

import java.io._
import ohnosequences.compota.aws.{MetapastaTestCredentials}
import ohnosequences.logging.ConsoleLogger
import ohnosequences.metapasta._
import ohnosequences.awstools.s3._
import ohnosequences.formats._
import ohnosequences.parsers._


class TestReads

case object TestReads {

  val file = ObjectAddress("metapasta-test", "microtest2.fastq")

  type Header = RawHeader
  type Read = FASTQ[Header]

  lazy val reads1000 = getReads(1000)
  lazy val reads10000 = getReads(10000)
  lazy val reads100000 = getReads(100000)

  def getReads(chunkSize: Long): List[String] = {
    val logger = new ConsoleLogger("testReads")
    MetapastaTestCredentials.aws match {
      case None => {
        logger.warn("aws credentials should be defined for this test")
        List[String]()
      }
      case Some(aws) => {

        val chunks = new S3Splitter(aws.s3.s3, file, chunkSize).chunks()
        val chunksNumber = chunks.length
        println(s"Parsing ${chunksNumber} chunks of size ${chunkSize}")

        val eraseCode = "\33[2K\r"
        val reader = S3ChunksReader(aws.s3.s3, file)
        val (reads, count) = chunks.foldLeft((List[String](), 0L)) { case ((acc, n), chunk) =>
          val (parsed, m) = reader.parseChunk[RawHeader](chunk._1, chunk._2)
          assert(parsed.length == m) // let's check this too by the way
          print(eraseCode)
          print((n + 1) + "/" + chunksNumber)
          (acc ++ parsed.map {
            _.header.toString
          }, n + 1) // only headers
        }
        print(eraseCode)
        println(s"Parsed ${reads.length} reads")
        reads
      }
    }
  }


}