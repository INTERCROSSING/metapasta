package ohnosequences.metapasta

import ohnosequences.nisperon.{AWS, MapInstructions}
import ohnosequences.formats.{RawHeader, FASTQ}
import ohnosequences.parsers.S3ChunksReader
import org.clapper.avsl.Logger

//todo it's very inefficient because of lack of batch
class ParseInstructions(aws: AWS) extends MapInstructions[List[ProcessedSampleChunk], List[ParsedSampleChunk]] {

  val logger = Logger(this.getClass)

  def apply(input: List[ProcessedSampleChunk]): List[ParsedSampleChunk] = {
    val chunk  = input.head

    logger.info("parsing " + chunk)
    val reader = S3ChunksReader(aws.s3, chunk.fastq)
    val parsed: List[FASTQ[RawHeader]] =
    reader.parseChunk[RawHeader](chunk.range._1, chunk.range._2)._1

    //parsed.map { fastq =>
    List(ParsedSampleChunk(chunk.name, parsed))
    //}
  }
}
