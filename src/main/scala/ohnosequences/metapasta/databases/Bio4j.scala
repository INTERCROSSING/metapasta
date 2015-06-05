package ohnosequences.metapasta

import java.io.File

import com.ohnosequences.bio4j.titan.model.util.{Bio4jManager, NodeRetrieverTitan}
import ohnosequences.awstools.s3.{LoadingManager, ObjectAddress}
import ohnosequences.logging.Logger
import ohnosequences.metapasta.databases.Installable

import scala.util.Try

class Bio4j(val nodeRetriever: com.ohnosequences.bio4j.titan.model.util.NodeRetrieverTitan) {}

object Bio4j {

  val bio4j201401 = fromS3(ObjectAddress("bio4j.releases", "ncbi_taxonomy"))

  def fromS3(bio4jLocation: ObjectAddress): Installable[Bio4j] = new Installable[Bio4j] {
    override def install(logger: Logger, workingDirectory: File, loadingManager: LoadingManager): Try[Bio4j] = {
      Try {
        logger.info("installing bio4j from location " + bio4jLocation)
      //  val bio4jDirectory = new File(workingDirectory, "bio4j")

        val dbDir = new File(new File(workingDirectory, "ncbi_taxonomy"), "v0.1.0")

        if (new File(dbDir, "00000067.jdb").exists()) {
          logger.warn(dbDir + " is already exists")
        } else {
          workingDirectory.mkdir()
          loadingManager.downloadDirectory(bio4jLocation, workingDirectory)
        }
        val bio4jManager = new Bio4jManager(dbDir.getAbsolutePath)
        new Bio4j(new NodeRetrieverTitan(bio4jManager))
      }
    }
  }
}

