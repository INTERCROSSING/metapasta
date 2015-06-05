package ohnosequences.metapasta.instructions

import java.io.{File, PrintWriter}

import ohnosequences.awstools.s3.LoadingManager
import ohnosequences.compota.MapInstructions
import ohnosequences.compota.environment.Env
import ohnosequences.formats.{FASTQ, RawHeader}
import ohnosequences.logging.Logger
import ohnosequences.metapasta.{AssignTable, MergedSampleChunk, _}
import ohnosequences.parsers.S3ChunksReader

import scala.util.{Success, Try}

class MappingInstructions(val mappingInstructionsConfiguration: AnyMappingInstructionsConfiguration) extends
MapInstructions[List[MergedSampleChunk], (AssignTable, Map[(String, AssignmentType), ReadsStats])] {

  case class MappingContext(
                             loadingManager: LoadingManager,
                             fastasWriter: Option[FastasWriter],
                             database: mappingInstructionsConfiguration.Database,
                             mappingTool: MappingTool[mappingInstructionsConfiguration.DatabaseReferenceId, mappingInstructionsConfiguration.Database],
                             assigner: Assigner[mappingInstructionsConfiguration.DatabaseReferenceId]
                             )

  override type Context = MappingContext


  override def prepare(env: Env): Try[MappingContext] = {
    val logger = env.logger
    val workingDirectory = env.workingDirectory

    mappingInstructionsConfiguration.loadingManager(logger).flatMap { loadingManager =>
      mappingInstructionsConfiguration.database.get(logger, workingDirectory, loadingManager).flatMap { database =>
        mappingInstructionsConfiguration.taxonRetriever.get(logger, workingDirectory, loadingManager).flatMap { taxonRetriever =>
          mappingInstructionsConfiguration.taxonomyTree.get(logger, workingDirectory, loadingManager).flatMap { tree =>
            mappingInstructionsConfiguration.mappingTool.get(logger, workingDirectory, loadingManager).flatMap { mappingTool =>
              mappingInstructionsConfiguration.fastaWriter.get(logger, workingDirectory, loadingManager).map { fastasWriter =>
                val assigner = new Assigner(
                  tree,
                  taxonRetriever,
                  mappingTool.extractReadId,
                  mappingInstructionsConfiguration.assignmentConfiguration,
                  fastasWriter
                )
                MappingContext(loadingManager, fastasWriter, database, mappingTool, assigner)
              }
            }
          }
        }
      }
    }
  }


  def saveParsedReads(logger: Logger, reads: List[FASTQ[RawHeader]], readsFile: File): Try[Boolean] = {
    Try {
      logger.info("saving reads to " + readsFile.getPath)
      val writer = new PrintWriter(readsFile)
      var emptyInput = true
      reads.foreach { fastq =>
        val s = fastq.toFasta
        if (emptyInput && !s.trim.isEmpty) {
          emptyInput = false
        }
        writer.println(s)
      }
      writer.close()
      emptyInput
    }
  }

  def apply(env: Env, context: MappingContext, input: List[MergedSampleChunk]): Try[(AssignTable, Map[(String, AssignmentType), ReadsStats])] = {

    import context._
    val logger = env.logger
    val workingDirectory = env.workingDirectory
    val emptyResults = (assignTableMonoid.unit, readStatMapMonoid.unit)

    input.headOption match {
      case None => {
        logger.warn("input is empty")
        Success(emptyResults)
      }
      case Some(chunk) => {
        //parsing
        val reader = S3ChunksReader(loadingManager.transferManager.getAmazonS3Client, chunk.fastq)
        val parsed: List[FASTQ[RawHeader]] = reader.parseChunk[RawHeader](chunk.range._1, chunk.range._2)._1
        val inputFile = new File(env.workingDirectory, "reads.fasta")
        saveParsedReads(logger, parsed, inputFile).flatMap { emptyInput =>
          val outputFile = new File(env.workingDirectory, "out.mapping")
          if (emptyInput) {
            logger.warn("empty chunk.. skipping mapping")
            Success(emptyResults)
          } else {
            logger.benchExecute("running " + mappingTool.name) {
              mappingTool.launch(logger, workingDirectory, database, inputFile, outputFile)
            }.flatMap { hits =>
              //todo add configs for it
              logger.uploadFile(outputFile, env.workingDirectory)
              Success(assigner.assign(logger, ChunkId(chunk), parsed, hits))
            }
          }
        }
      }
    }
  }
}
