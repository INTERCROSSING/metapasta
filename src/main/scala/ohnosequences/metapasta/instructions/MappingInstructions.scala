package ohnosequences.metapasta.instructions

import java.io.{File, PrintWriter}

import ohnosequences.awstools.s3.LoadingManager
import ohnosequences.compota.{AWS, MapInstructions}
import ohnosequences.formats.{FASTQ, RawHeader}
import ohnosequences.logging.Logger
import ohnosequences.metapasta.{AssignTable, MergedSampleChunk, _}
import ohnosequences.parsers.S3ChunksReader

import scala.util.{Success, Try}

class MappingInstructions(val configuration: MetapastaConfiguration) extends
MapInstructions[List[MergedSampleChunk], (AssignTable, Map[(String, AssignmentType), ReadsStats])] {

  case class MappingContext(
                             loadingManager: LoadingManager,
                             database: configuration.Database,
                             mappingTool: MappingTool[configuration.DatabaseReferenceId, configuration.Database],
                             assigner: Assigner[configuration.DatabaseReferenceId]
                             )

  override type Context = MappingContext

  override def prepare(logger: Logger, workingDirectory: File, aws: AWS): Try[MappingContext] = {

    val loadingManager = aws.s3.createLoadingManager()

    configuration.database.get(logger, workingDirectory, loadingManager).flatMap { database =>
      configuration.taxonRetriever.get(logger, workingDirectory, loadingManager).flatMap { taxonRetriever =>
        configuration.taxonomy.get(logger, workingDirectory, loadingManager).flatMap { taxonomy =>
          configuration.mappingTool.get(logger, workingDirectory, loadingManager).map { mappingTool =>
            val fastasWriter = new FastasWriter[configuration.DatabaseReferenceId](aws, configuration, taxonomy)
            val assigner = new Assigner(
              taxonomy.tree,
              taxonRetriever,
              mappingTool.extractReadId,
              configuration.assignmentConfiguration,
              Some(fastasWriter)
            )
            MappingContext(loadingManager, database, mappingTool, assigner)
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


  override def apply(logger: Logger, workingDirectory: File, context: MappingContext, input: List[MergedSampleChunk]): Try[(AssignTable, Map[(String, AssignmentType), ReadsStats])] = {

    import context._


    val emptyResults = (assignTableMonoid.unit, readStatMapMonoid.unit)

    input.headOption match {
      case None => {
        logger.warn("input is empty")
        Success(emptyResults)
      }
      case Some(chunk) => {
        //parsing
        val reader = S3ChunksReader(loadingManager.transferManager.getAmazonS3Client, chunk.fastq)
        val parsed: List[FASTQ[RawHeader]] = reader.parseChunk[RawHeader](chunk.start, chunk.end)._1
        val inputFile = new File(workingDirectory, "reads.fasta")
        saveParsedReads(logger, parsed, inputFile).flatMap { emptyInput =>
          val outputFile = new File(workingDirectory, "out.mapping")
          if (emptyInput) {
            logger.warn("empty chunk.. skipping mapping")
            Success(emptyResults)
          } else {
            logger.benchExecute("running " + mappingTool.name) {
              mappingTool.launch(logger, workingDirectory, database, inputFile, outputFile)
            }.flatMap { hits =>
              //todo add configs for it
              logger.uploadFile(outputFile, workingDirectory)
              Success(assigner.assign(logger, chunk.chunkId, parsed, hits))
            }
          }
        }
      }
    }
  }
}