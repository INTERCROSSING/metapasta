package ohnosequences.metapasta.databases

import java.io._
import java.net.URL
import java.util.concurrent.atomic.AtomicReference

import ohnosequences.awstools.s3.LoadingManager
import ohnosequences.logging.Logger
import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveInputStream}
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}


trait Installable[T] {

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

  def extractTarGz(logger: Logger, gzippedFile: File, workingDirectory: File, destination: File): Try[File] = {

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
        def copy(inputStream: InputStream, outputStream: OutputStream, bufferSize: Int = 4000): Unit = {
          val buffer = new Array[Byte](bufferSize)
          inputStream.read(buffer) match {
            case -1 => ()
            case len => {
              outputStream.write(buffer, 0, len)
              copy(inputStream, outputStream, bufferSize)
            }
          }
        }


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
              copy(tarStream, outputStream)
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

  val cachedResults = new AtomicReference[Try[T]](Failure(new Error("initialization error")))

  def get(logger: Logger, workingDirectory: File, loadingManager: LoadingManager): Try[T] = {
    cachedResults.get() match {
      case Success(t) => Success(t)
      case _ => {
        val v = install(logger, workingDirectory, loadingManager)
        cachedResults.set(v)
        v
      }
    }
  }

  def install(logger: Logger, workingDirectory: File, loadingManager: LoadingManager): Try[T]

}
