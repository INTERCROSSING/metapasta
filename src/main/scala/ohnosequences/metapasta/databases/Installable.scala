package ohnosequences.metapasta.databases

import ohnosequences.awstools.s3.LoadingManager
import ohnosequences.logging.Logger

import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveInputStream}
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

import java.io._
import java.net.URL
import java.util.concurrent.atomic.AtomicReference

/**
 * Dependency that can on request
 */
trait Installable[T] {

  private val value = new AtomicReference[Try[T]](Failure(new Error("initialization error")))

  def get(logger: Logger, workingDirectory: File, loadingManager: LoadingManager): Try[T] = {
    value.get() match {
      case Success(t) => Success(t)
      case _ => {
        val v = install(logger, workingDirectory, loadingManager)
        value.set(v)
        v
      }
    }
  }

  protected def install(logger: Logger, workingDirectory: File, loadingManager: LoadingManager): Try[T]

}

object Installable {

  def extractGZ(logger: Logger, gzFile: File, destination: File): Try[File] = {
    Try {
      val inputFileStream = new FileInputStream(gzFile)
      val bufferedStream = new BufferedInputStream(inputFileStream)
      val gzippedStream = new GzipCompressorInputStream(bufferedStream)

      val outputFileStream = new FileOutputStream(destination)
      copy(gzippedStream, outputFileStream)
      inputFileStream.close()
      outputFileStream.close()
      destination
    }.recoverWith { case t =>
      Failure(new Error("can't gunzip the archive " + gzFile.getAbsolutePath))
    }

  }

  def copy(inputStream: InputStream, outputStream: OutputStream, bufferSize: Int = 4000): Unit = {

    val buffer = new Array[Byte](bufferSize)

    @tailrec
    def copy(inputStream: InputStream): Unit = {
      inputStream.read(buffer) match {
        case -1 => ()
        case len => {
          outputStream.write(buffer, 0, len)
          copy(inputStream)
        }
      }
    }

    copy(inputStream)
  }



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

  def extractTarGz(logger: Logger, gzippedFile: File, destination: File): Try[File] = {

    Try {
      if (destination.exists()) {
        logger.warn(destination.getAbsolutePath + " already exists")
        destination
      } else {

        destination.mkdir()

        val fileStream = new FileInputStream(gzippedFile)
        val bufferedStream = new BufferedInputStream(fileStream)
        val gzippedStream = new GzipCompressorInputStream(bufferedStream)
        val tarStream = new TarArchiveInputStream(gzippedStream)


        @tailrec
        def processTarEntries(tarArchiveEntry: Option[TarArchiveEntry]): Unit = {
          tarArchiveEntry match {
            case None => {
              ()
            }
            case Some(entry) if entry.isDirectory => {
              val dst = new File(destination, entry.getName)
              logger.info("creating directory " + dst.getAbsolutePath)
              dst.mkdir()
              processTarEntries(Option(tarStream.getNextTarEntry))
            }
            case Some(entry) => {
              val dst = new File(destination, entry.getName)
              logger.info("extracting " + dst.getAbsolutePath)
              val outputStream = new BufferedOutputStream(new FileOutputStream(dst))
              Installable.copy(tarStream, outputStream)
              outputStream.close()
              processTarEntries(Option(tarStream.getNextTarEntry))
            }
          }
        }

        processTarEntries(Option(tarStream.getNextTarEntry))
        destination
      }
    }
  }


}