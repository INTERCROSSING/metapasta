package ohnosequences.metapasta.instructions


import ohnosequences.awstools.s3.{LoadingManager, ObjectAddress}
import ohnosequences.compota.{AWS, Instructions}
import ohnosequences.logging.Logger
import ohnosequences.metapasta.databases.Installable
import ohnosequences.metapasta.{MergedSampleChunk, PairedSample, ReadsStats, _}

import scala.util.{Failure, Success, Try}

import java.io.File

class MergingInstructions(configuration: MergingInstructionsConfiguration, s3Paths: S3Paths)
  extends Instructions[List[PairedSample], (List[MergedSampleChunk], Map[(String, AssignmentType), ReadsStats])] {

  case class MergingContext(loadingManager: LoadingManager, mergingTool: MergingTool)

  override type Context = MergingContext

  override def prepare(logger: Logger, workingDirectory: File, aws: AWS): Try[Context] = {
    val loadingManager = aws.s3.createLoadingManager()
    configuration.mergingTool.get(logger, workingDirectory, loadingManager).map { mergingTool =>
      MergingContext(loadingManager, mergingTool)
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
      logger.info("downloadUnpackUpload file:" + file)
      val readsFile = new File(workingDirectory, file)

      if (source.key.endsWith(".gz")) {
        val archiveFile = new File(workingDirectory, file + ".gz")
        logger.info("downloading " + source + " to " + archiveFile.getAbsolutePath)
        loadingManager.download(source, archiveFile)
        logger.info("extracting " + archiveFile.getAbsolutePath)
        Installable.extractGZ(logger, archiveFile, readsFile)
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

  def solve(logger: Logger, workingDirectory: File, context: Context, input: List[PairedSample]): Try[List[(List[MergedSampleChunk], Map[(String, AssignmentType), ReadsStats])]] = {

    val loadingManager = context.loadingManager

    val sample = input.head

    val mergedReadsObject = s3Paths.mergedReadsDestination(sample.id)

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
            val notMergedDst = s3Paths.notMergedReadsDestination(sample.id)
            mergeResults.notMerged1.foreach { file =>
              loadingManager.upload(notMergedDst._1, file)
            }
            mergeResults.notMerged2.foreach { file =>
              loadingManager.upload(notMergedDst._2, file)
            }
            loadingManager.upload(mergedReadsObject, mergeResults.merged)
            mergeResults.stats
          }
        }
      }
    }).map { stats =>
      logger.info("splitting " + mergedReadsObject)
      val rawChunks = new S3Splitter(loadingManager.transferManager.getAmazonS3Client, mergedReadsObject, configuration.chunksSize).chunks()
      val chunks = configuration.chunksThreshold match {
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
          (List(MergedSampleChunk(mergedReadsObject, sample.id, chunk._1, chunk._2)), Map((sample.name, BBH) -> stats, (sample.name, LCA) -> stats))
        }
        case (chunk, _) => {
          (List(MergedSampleChunk(mergedReadsObject, sample.id, chunk._1, chunk._2)), readStatMapMonoid.unit)
        }
      }
      res
    }
  }
}