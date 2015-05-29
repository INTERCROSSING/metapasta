package ohnosequences.metapasta.instructions

import ohnosequences.awstools.s3.{LoadingManager, S3, ObjectAddress}
import java.io.File
import ohnosequences.compota.Instructions
import ohnosequences.compota.environment.Env
import ohnosequences.formats.FASTQ
import ohnosequences.logging.Logger
import ohnosequences.metapasta._
import ohnosequences.metapasta.MergedSampleChunk
import ohnosequences.metapasta.PairedSample
import ohnosequences.metapasta.ReadsStats

import scala.util.{Failure, Success, Try}


class MergingInstructions(metapastaConfiguration: MetapastaConfiguration)
  extends Instructions[List[PairedSample], (List[MergedSampleChunk], Map[(String, AssignmentType), ReadsStats])] {

  case class MergingContext(loadingManager: LoadingManager, mergingTool: MergingTool)

  override type Context = MergingContext

  override def prepare(env: Env): Try[Context] =  {
    import env._

    metapastaConfiguration.loadingManager(logger).flatMap { loadingManager =>
      metapastaConfiguration.mergingTool(logger, workingDirectory, loadingManager).map { mergingTool =>
        MergingContext(loadingManager, mergingTool)
      }
    }
  }


  //todo do it more precise
  def countReads(file: File): Try[Long] = {
    var count = 0L
    Try {
      scala.io.Source.fromFile(file).getLines().foreach {
        str => count += 1
      }
      count / 4
    }
  }

  def downloadUnpackUpload(logger: Logger,
                           loadingManager: LoadingManager,
                           source: ObjectAddress,
                           workingDirectory: File,
                           file: String,
                           destination: Option[ObjectAddress]
  ): Try[File] = {

    Success(()).flatMap { u =>
      val readsFile = new File(workingDirectory, file)

      if (source.key.endsWith(".gz")) {
        val archiveFile = new File(workingDirectory, file + ".gz")
        logger.info("downloading " + source + " to " + archiveFile.getAbsolutePath)
        loadingManager.download(source, archiveFile)
        logger.info("extracting " + archiveFile.getAbsolutePath)
        val gzipCommand = Seq("gunzip", archiveFile.getAbsolutePath)
        sys.process.Process(gzipCommand, workingDirectory).! match {
          case 0 => {
            Success(readsFile)
          }
          case _ => {
            Failure(new Error("can't gunzip the archive " + archiveFile.getAbsolutePath))
          }
        }
      } else {
        logger.info("downloading " + source + " to " + readsFile.getAbsolutePath)
        loadingManager.download(source, readsFile)
        Success(readsFile)
      }
    }.flatMap { readsFile =>
      destination.foreach { dst =>
        logger.info("uploading " + readsFile.getAbsolutePath + " to " + dst)
        loadingManager.upload(dst, readsFile)
      }
      Success(readsFile)
    }
  }

  def solve(env: Env, context: Context, input: List[PairedSample]): Try[List[( List[MergedSampleChunk], Map[(String, AssignmentType), ReadsStats])]] = {

    val logger = env.logger
    val loadingManager = context.loadingManager
    val workingDirectory = env.workingDirectory

    val sample = input.head

    val mergedReadsObject = metapastaConfiguration.mergedReadsDestination(sample)

    (if (sample.fastq1.equals(sample.fastq2)) {
      logger.info("paired-end sample")
      downloadUnpackUpload(logger, loadingManager, sample.fastq1, workingDirectory, "1.fastq", Some(mergedReadsObject)).flatMap { readsFile =>
        countReads(readsFile).map { totalReads =>
          val statBuilder = new ReadStatsBuilder()
          statBuilder.merged = totalReads
          statBuilder.total = totalReads
          statBuilder.build
        }
      }
    } else {
      downloadUnpackUpload(logger, loadingManager, sample.fastq1, workingDirectory, "1.fastq", None).flatMap { readsFile1 =>
        downloadUnpackUpload(logger, loadingManager, sample.fastq2, workingDirectory, "2.fastq", None).flatMap { readsFile2 =>
          logger.info(context.mergingTool.name + ": merging " + readsFile1.getAbsolutePath + " and " + readsFile2.getAbsolutePath)
          context.mergingTool.merge(logger, workingDirectory, readsFile1, readsFile2).map { mergeResults =>
            val notMergedDst = metapastaConfiguration.notMerged(sample)
            mergeResults.notMerged1.foreach { notMerged1 =>
              loadingManager.upload(notMergedDst._1, notMerged1)
            }
            mergeResults.notMerged2.foreach { notMerged1 =>
              loadingManager.upload(notMergedDst._2, notMerged1)
            }
            loadingManager.upload(mergedReadsObject, mergeResults.merged)
            mergeResults.stats
          }
        }
      }
    }).map { stats =>
      logger.info("splitting " + mergedReadsObject)
      val rawChunks = new S3Splitter(loadingManager.transferManager.getAmazonS3Client, mergedReadsObject, metapastaConfiguration.chunksSize).chunks()
      val chunks = metapastaConfiguration.chunksThreshold match {
        case None => {
          rawChunks
        }
        case Some(n) => {
          logger.warn("chunk threshold " + n)
          rawChunks.take(n)
        }
      }
      val res: List[(List[MergedSampleChunk], Map[(String, AssignmentType), ReadsStats])] = chunks.zipWithIndex.map {
        case (chunk, 0) => {
          (List(MergedSampleChunk(mergedReadsObject, sample.name, chunk)), Map((sample.name, BBH) -> stats, (sample.name, LCA) -> stats))
        }
        case (chunk, _) => {
          (List(MergedSampleChunk(mergedReadsObject, sample.name, chunk)), readStatMapMonoid.unit)
        }
      }
      res
    }
  }
}
