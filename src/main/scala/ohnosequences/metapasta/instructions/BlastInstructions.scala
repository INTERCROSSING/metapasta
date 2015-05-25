package ohnosequences.metapasta.instructions

import java.io.{File, PrintWriter}
import ohnosequences.compota.MapInstructions
import ohnosequences.compota.environment.Env
import ohnosequences.logging.Logger

import ohnosequences.metapasta._
import ohnosequences.metapasta.MergedSampleChunk
import ohnosequences.parsers.S3ChunksReader
import ohnosequences.formats.RawHeader
import ohnosequences.formats.FASTQ
import ohnosequences.metapasta.AssignTable
import ohnosequences.awstools.s3.{LoadingManager}

import scala.util.{Success, Try}

class MappingInstructions(metapastaConfiguration: MetapastaConfiguration) extends
   MapInstructions[List[MergedSampleChunk],  (AssignTable, Map[(String, AssignmentType), ReadsStats])] {

  case class MappingContext(
                             loadingManager: LoadingManager,
                             bio4j: Bio4j,
                             database: metapastaConfiguration.Database,
                             mappingTool: MappingTool[metapastaConfiguration.DatabaseReferenceId, metapastaConfiguration.Database],
                             assigner: Assigner[metapastaConfiguration.DatabaseReferenceId]
                             )

  override type Context = MappingContext

  override def prepare(env: Env): Try[MappingContext] = {
    val logger = env.logger
    metapastaConfiguration.loadingManager(logger).flatMap { loadingManager =>
      metapastaConfiguration.bio4j(logger, loadingManager).flatMap { bio4j =>
        metapastaConfiguration.mappingDatabase(logger, loadingManager).flatMap { blastDatabase =>
          metapastaConfiguration.taxonRetriever(logger, loadingManager).flatMap { taxonRetriever =>
            metapastaConfiguration.taxonomyTree(logger, loadingManager, bio4j).flatMap { tree =>
              metapastaConfiguration.mappingTool(logger, loadingManager).map { mappingTool =>
                val fastasWriter = new FastasWriter(loadingManager, metapastaConfiguration.readDirectory, bio4j)
                val assigner = new Assigner(
                  tree,
                  blastDatabase,
                  taxonRetriever, metapastaConfiguration.assignmentConfiguration,
                  metapastaConfiguration.extractReadHeader,
                  Some(fastasWriter)
                )
                MappingContext(loadingManager, bio4j, blastDatabase, mappingTool, assigner)
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
              mappingTool.launch(logger, database, inputFile, outputFile)
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
