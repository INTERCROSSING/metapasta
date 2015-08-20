package ohnosequences.metapasta.instructions

import java.io.File
import java.net.URL

import ohnosequences.awstools.s3.{ObjectAddress, LoadingManager}
import ohnosequences.logging.Logger
import ohnosequences.metapasta.{Utils, Hit, ReadId}
import ohnosequences.metapasta.databases.{Installable, BlastDatabase16S, ReferenceId}

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

class Blast[R <: ReferenceId, D <: BlastDatabase16S[R]](blastn: File, blastTemplate: List[String]) extends MappingTool[R, D] {
  
  override val name: String = "blast"

  override def extractReadId(header: String): ReadId = ReadId(header)


  override def launch(logger: Logger, workingDirectory: File, database: D, readsFile: File, outputFile: File): Try[List[Hit[R]]] = {
    val commandRaw = blastTemplate.map { arg =>
      arg
        .replace("$db$", database.blastParameter)
        .replace("$input$", readsFile.getAbsolutePath)
        .replace("$output$", outputFile.getAbsolutePath)
        .replace("$out_format$", "6")
        .replace("$threads_count$", Runtime.getRuntime().availableProcessors().toString)
    }


    val command = List(blastn.getAbsolutePath) ++ commandRaw
    logger.info("blast command: " + command)
    val res = logger.benchExecute("executing BLAST") {
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
                logger.warn("couldn't parse reference id from BLAST output " + refIdRaw)
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

  //ManagementFactory.getThreadMXBean().getThreadCount()

  def defaultBlastnTemplate = List(
    "-task",
    "megablast",
    "-db",
    "$db$",
    "-query",
    "$input$",
    "-out",
    "$output$",
    "-max_target_seqs",
    "1",
    "-num_threads",
    "$threads_count$",
    "-outfmt",
    "$out_format$",
    "-show_gis"
  )

  import Installable._


  def windows[R <: ReferenceId](blastTemplate: List[String]): Installable[MappingTool[R, BlastDatabase16S[R]]] = new Installable[MappingTool[R, BlastDatabase16S[R]]] {
    override def install(logger: Logger, workingDirectory: File, loadingManager: LoadingManager): Try[MappingTool[R, BlastDatabase16S[R]]] = {

      val latestURL = new URL( """ftp://ftp.ncbi.nlm.nih.gov/blast/executables/blast+/LATEST/ncbi-blast-2.2.30+-ia32-win32.tar.gz""")
      val blastArchive = new File(workingDirectory, "ncbi-blast-2.2.30+-ia32-win32.tar.gz")
      workingDirectory.mkdir()
      download("blast", logger, latestURL, blastArchive).flatMap { archive =>
        extractTarGz(logger, archive, new File(workingDirectory, "blast")).map { dst =>
          logger.info("installing BLAST")
          val blastRoot = new File(dst, "ncbi-blast-2.2.30+")
          val blastBin = new File(blastRoot, "bin")
          val blastn = new File(blastBin, "blastn")
          blastn.setExecutable(true)
          new Blast[R, BlastDatabase16S[R]](blastn, blastTemplate)
        }
      }
    }

  }


  def linux[R <: ReferenceId](blastTemplate: List[String]): Installable[MappingTool[R, BlastDatabase16S[R]]] = new Installable[MappingTool[R, BlastDatabase16S[R]]] {
    override def install(logger: Logger, workingDirectory: File, loadingManager: LoadingManager): Try[MappingTool[R, BlastDatabase16S[R]]] = {
      logger.info("downloading BLAST")
      val blast = ObjectAddress("resources.ohnosequences.com", "blast/ncbi-blast-2.2.25.tar.gz")
      workingDirectory.mkdir()
      val blastArchive = new File(workingDirectory, "ncbi-blast-2.2.25.tar.gz")
      loadingManager.download(blast, blastArchive)
      logger.info("installing BLAST")

      extractTarGz(logger, blastArchive, new File(workingDirectory, "blast")).map { dst =>
        val blastRoot = new File(dst, "ncbi-blast-2.2.30+")
        val blastBin = new File(blastRoot, "bin")
        val blastn = new File(blastBin, "blastn")
        blastn.setExecutable(true)
        new Blast[R, BlastDatabase16S[R]](blastn, blastTemplate)
      }

    }
  }
}