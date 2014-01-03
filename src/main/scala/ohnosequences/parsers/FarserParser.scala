package ohnosequences.parsers

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.GetObjectRequest
import com.amazonaws.services.s3.model.S3Object

import ohnosequences.awstools._
import ohnosequences.awstools.s3._

import java.io._
import scala.io._

import ohnosequences.formats._

/* Type class which
- requires to define header parser
- provides parsers for reads
*/
trait ParsableHeaders[H <: Header] {
  def headerParser(line: String): Option[H]

  // "parses" one read/item/record
  def itemParser(ls: Seq[String]): Option[FASTQ[H]] = {
    headerParser(ls(0)) match {
      case Some(h) if (
        ls.length == 4 &&             // 4 lines
          ls(2).startsWith("+") &&      // third is opt-header
          ls(1).length == ls(3).length  // sequence length == quality lienght
        ) => Some(FASTQ(h, ls(1), ls(2), ls(3)))
      case _ => None
    }
  }
}

/* Implicit instances for know headers */
object ParsableHeaders {
  implicit object RawHeaderParser extends ParsableHeaders[RawHeader] {
    def headerParser(line: String) = if (line.startsWith("@")) Some(RawHeader(line)) else None
  }

  implicit object CasavaHeaderParser extends ParsableHeaders[CasavaHeader] {
    def headerParser(line: String) = {
      // @<instrument>:<run number>:<flowcell ID>:<lane>:<tile>:<x-pos>:<y-pos> <read>:<is filtered>:<control number>:<index sequence>
      // @EAS139:136:FC706VJ:2:2104:15343:197393 1:Y:18:ATCACG
      val casabaPattern = """@([\w\-]+):(\d+):(\w+):(\d+):(\d+):(\d+):(\d+)\s+(\d+):(\w):(\d+):(\w+)""".r
      line match {
        case casabaPattern(instrument, runNumber, flowcellID, lane, tile, xPos, yPos, read, isFiltered, controlNumber, indexSequence) =>
          Some(CasavaHeader(line)(instrument, runNumber.toLong, flowcellID, lane.toLong, tile.toLong, xPos.toLong, yPos.toLong, read.toLong, isFiltered, controlNumber.toLong, indexSequence))
        case _ => None
      }
    }
  }
}

/* Class which works reads S3 by chunks and parses them */
case class S3ChunksReader(s3: S3, objectAddress: ObjectAddress) {

  def readChunk(start: Long, end: Long): Iterator[String] = {
    if (start >= end) return Nil.toIterator

    val request = new GetObjectRequest(objectAddress.bucket, objectAddress.key).withRange(start, end)
    try {
      val objectPortion: S3Object = s3.s3.getObject(request)
      val objectStream = objectPortion.getObjectContent
      Source.fromInputStream(objectStream).getLines()
    } catch {
      case e: com.amazonaws.AmazonServiceException
        if e.getErrorCode == "InvalidRange" => Nil.toIterator
    }
  }

  type ParsedLength = Long
  type ParsedNumber = Long

  // takes an iterator over lines and returns:
  // (parsed reads, length of prased piece, number of parsed reads)
  def readsParser[H <: Header : ParsableHeaders](strm: Iterator[String]):
  (List[FASTQ[H]], ParsedLength, ParsedNumber) = {

    val parser = implicitly[ParsableHeaders[H]].itemParser _

    // skip some trash in the beginning:
    val (trash, slider) = strm.sliding(4).span{ parser(_).isEmpty }
    val trashText = trash.flatMap(_.take(1)).mkString("\r")
    val trashLength: ParsedLength = trashText.length + trash.take(1).length
    // println("trash:      " + trashLength)
    // println(trashText)

    // sliding, parsing accumulating:
    slider.foldLeft(List[FASTQ[H]](), trashLength, 0L) {
      case ((acc, l, n), gr) =>
        parser(gr) match {
          case Some(read) =>
            ( read :: acc
              , l + gr.mkString("\r").length + 1
              , n + 1
              )
          case _ => (acc, l, n)
        }
    }
  }

  def parseChunk[H <: Header : ParsableHeaders](start: Long, end: Long):
  (List[FASTQ[H]], ParsedNumber) = {
    val (reads, l, n) = readsParser(readChunk(start, end))
    val stopPoint = start + l
    val avgLength = if (n == 0L) 0 else l / n
    val anotherPiece = if (stopPoint == end) 0 else avgLength
    // println("start:     " + start)
    // println("stopPoint: " + stopPoint)
    // println("end:       " + end)
    // println("diff:      " + (end - stopPoint))
    // println("avgLength: " + avgLength)
    val (rest, _, _) = readsParser(readChunk(stopPoint, end + anotherPiece))
    val mbLast = rest.take(1)
    (reads ++ mbLast, n + mbLast.length)
  }
}