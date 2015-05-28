package ohnosequences.metapasta.instructions

import java.io.File
import java.net.URL

import ohnosequences.awstools.s3.{ObjectAddress, LoadingManager}
import ohnosequences.logging.Logger
import ohnosequences.metapasta.{RefId, ReadId, Utils, Hit}
import ohnosequences.metapasta.databases.{GI, BlastDatabase16S, Database16S, ReferenceId}
import org.apache.commons.io.FileUtils

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}


trait AnyMappingTool {

  type Reference <: ReferenceId

  type Database <: Database16S[Reference]

  val database: Database

  def extractReadId(header: String): ReadId

  def launch(logger: Logger,  workingDirectory: File, database: Database, readsFile: File, outputFile: File): Try[List[Hit[Reference]]]

  val name: String
}


abstract class MappingTool[R <: ReferenceId, D <: Database16S[R]] extends AnyMappingTool {
  override type Reference = R
  override type Database = D

}

class Blast[R <: ReferenceId, D <: Database16S[R]](blastBin: File, blastTemplate: List[String], val database: D) extends MappingTool[R, D] {


  override val name: String = "blast"

  override def extractReadId(header: String): ReadId = ReadId(header)


  override def launch(logger: Logger, workingDirectory: File, database: D, readsFile: File, outputFile: File): Try[List[Hit[R]]] = {
    val commandRaw =  blastTemplate.map { arg =>
      arg
        .replace("$db$", database.name)
        .replace("$output$", readsFile.getAbsolutePath)
        .replace("$input$", outputFile.getAbsolutePath)
        .replace("$out_format$", "6")
    }

    val command = List(new File(blastBin, commandRaw.head).getAbsolutePath) ++ commandRaw.tail
    val res =  logger.benchExecute("executing BLAST " + command) {
      sys.process.Process(command, workingDirectory).! match {
        case 0 => {
          Success(outputFile)
        }
        case _ => {
          Failure(new Error("can't execute BLAST"))
        }
      }
    }

    res.map { blastOutput =>
      //blast 12 fields
      //25  gi|339283447|gb|JF799642.1| 100.00  399 0   0   1   399 558 956 0.0  737
      //M02255:17:000000000-A8J9J:1:2104:18025:8547     gi|291331518|gb|GU958050.1|     88.31   77      3       6       1       74      506     579     1e-15   87.9
      //val blastHit = """\s*([^\s]+)\s*([^\s]+)\s*([^\s]+)\s*([^\s]+)\s*([^\s]+)\s*([^\s]+)\s*([^\s]+)\s*([^\s]+)\s*([^\s]+)\s*([^\s]+)\s*([^\s]+)\s*(\d+)$""".r
      val blastHit = """^\s*([^\s]+)\s+([^\s]+).*?([^\s]+)\s*$""".r
      val comment = """#(.*)""".r

      logger.benchExecute("parsing BLAST output from " + blastOutput) {
        val hits = new ListBuffer[Hit[R]]()
        scala.io.Source.fromFile(blastOutput).getLines().foreach {
          case comment(c) => //logger.info("skipping comment: " + c)
          case blastHit(header, refIdRaw, _score) => {
            val readId = extractReadId(header)
            database.parseRawId(refIdRaw) match {
              case None => {
                logger.warn("coun't parse reference id from BLAST output " + refIdRaw)
              }
              case Some(refId) => {
                val score = Utils.parseDouble(_score)
                hits += Hit(readId, refId, score)
              }
            }


          }
          case l => logger.error("can't parse: " + l)
        }
        hits.toList
      }
    }
  }
}

object Blast {

  def download(name: String, logger: Logger, url: URL, file: File): Try[File] = {
    import sys.process._
    logger.info("downloading " + name + " from " + url)
    if (!file.exists()) {
      (url #> file).! match {
        case 0 => {
          logger.info("downloaded")
          Success(file)
        }
        case _ => {
          val e = new Error("couldn't download " + url.toString)
          logger.error(e)
          Failure(e)
        }
      }
    } else {
      logger.info(file.getName + " already downloaded")
      Success(file)
    }
  }

  def extract(logger: Logger, gzippedFile: File, workingDirectory: File, destination: File): Try[File] = {
    logger.info("extracting " + gzippedFile.getAbsolutePath)

    val tarCommand = Seq("tar", "-xvf", gzippedFile.getAbsolutePath, "-C", destination.getAbsolutePath)
    logger.info("running tar: " + tarCommand)
    sys.process.Process(tarCommand, workingDirectory).! match {
      case 0 => {
        Success(destination)
      }
      case _ => {
        val e = new Error("can't extract " + gzippedFile.getAbsolutePath)
        logger.error(e)
        Failure(e)
      }
    }
  }

  def windows[R <: ReferenceId](logger: Logger, loadingManager: LoadingManager, workingDirectory: File, blastTemplate: List[String], blastDatabase: BlastDatabase16S[R]): Try[Blast[R, BlastDatabase16S[R]]] = {
    val latestURL = new URL("""ftp://ftp.ncbi.nlm.nih.gov/blast/executables/blast+/LATEST/ncbi-blast-2.2.30+-ia32-win32.tar.gz""")
    val blastArchive = new File(workingDirectory, "ncbi-blast-2.2.30+-ia32-win32.tar.gz")

    download("blast", logger, latestURL, blastArchive).flatMap { archive =>
      extract(logger, archive, workingDirectory, new File(workingDirectory, "blast")).map { dst =>
        logger.info("installing BLAST")
        val blastBin = new File(dst, "bin")
        val blastn = new File(blastBin, "blastn")
        blastn.setExecutable(true)
        new Blast(blastBin, blastTemplate, blastDatabase)
      }
    }
  }

  def linux[R <: ReferenceId](logger: Logger, loadingManager: LoadingManager, workingDirectory: File, blastTemplate: List[String], blastDatabase: BlastDatabase16S[R]): Try[Blast[R, BlastDatabase16S[R]]] = {

    Try {
      logger.info("downloading BLAST")
      val blast = ObjectAddress("resources.ohnosequences.com", "blast/ncbi-blast-2.2.25.tar.gz")
      val blastArchive = new File(workingDirectory, "ncbi-blast-2.2.25.tar.gz")
      loadingManager.download(blast, blastArchive)
      blastArchive
    }.flatMap { blastArchive =>
      logger.info("extracting BLAST")
      val tarCommand = Seq("tar", "-xvf", blastArchive.getAbsolutePath)
      logger.info("extracting BLAST " + tarCommand)
      sys.process.Process(tarCommand, workingDirectory).! match {
        case 0 => {
          Success(new File(new File(workingDirectory, "ncbi-blast-2.2.25+"), "bin"))
        }
        case _ => {
          Failure(new Error("can't extract BLAST"))
        }
      }
    }.map { blastBin =>
      logger.info("installing BLAST")
      // new Runtime().exec()
      val blastn = new File(blastBin, "blastn")

      blastn.setExecutable(true)
      new Blast(blastBin, blastTemplate, blastDatabase)
    }
  }
}