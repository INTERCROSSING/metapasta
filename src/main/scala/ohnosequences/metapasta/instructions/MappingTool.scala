package ohnosequences.metapasta.instructions

import java.io.File

import ohnosequences.logging.Logger
import ohnosequences.metapasta.{RefId, ReadId, Utils, Hit}
import ohnosequences.metapasta.databases.{Database16S, ReferenceId}

import scala.collection.mutable.ListBuffer
import scala.util.Try


trait AnyMappingTool {

  type Reference <: ReferenceId

  type Database <: Database16S[Reference]

  def launch(logger: Logger, database: Database, readsFile: File, outputFile: File): Try[List[Hit]]

  val name: String
}


abstract class MappingTool[R <: ReferenceId, D <: Database16S[R]] extends AnyMappingTool {
  override type Reference = R
  override type Database = D

}

class Blast[R <: ReferenceId, D <: Database16S[R]] extends MappingTool[R, D] {
  override def launch(logger: Logger, database: D, readsFile: String, template: String): Try[List[Hit]] = {
    logger.info("reading BLAST result")
    //todo add xml parser
    val resultRaw = if (useXML) "" else Utils.readFile(outputFile)

    val t1 = System.currentTimeMillis()


    //blast 12 fields
    //25  gi|339283447|gb|JF799642.1| 100.00  399 0   0   1   399 558 956 0.0  737
    //M02255:17:000000000-A8J9J:1:2104:18025:8547     gi|291331518|gb|GU958050.1|     88.31   77      3       6       1       74      506     579     1e-15   87.9
    //val blastHit = """\s*([^\s]+)\s*([^\s]+)\s*([^\s]+)\s*([^\s]+)\s*([^\s]+)\s*([^\s]+)\s*([^\s]+)\s*([^\s]+)\s*([^\s]+)\s*([^\s]+)\s*([^\s]+)\s*(\d+)$""".r
    val blastHit = """^\s*([^\s]+)\s+([^\s]+).*?([^\s]+)\s*$""".r

    val comment = """#(.*)""".r


    val hits = new ListBuffer[Hit]()
    resultRaw.linesIterator.foreach {
      case comment(c) => //logger.info("skipping comment: " + c)
      case blastHit(header, refId, _score) => {
        val readId = extractHeader(header)
        val score = Utils.parseDouble(_score)
        hits += Hit(ReadId(readId), RefId(refId), score)
      }
      case l => logger.error("can't parse: " + l)
    }

    val t2 = System.currentTimeMillis()
    logger.info("parsed " + hits.size + " hits " + (t2 - t1) + " ms")

  }
}