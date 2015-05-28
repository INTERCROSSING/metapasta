package ohnosequences.metapasta

import java.io.File

import com.ohnosequences.bio4j.titan.model.util.{NodeRetrieverTitan, Bio4jManager}
import ohnosequences.awstools.s3.{LoadingManager, S3, ObjectAddress}
import ohnosequences.logging.Logger

import scala.util.Try

class Bio4j(val nodeRetriever: com.ohnosequences.bio4j.titan.model.util.NodeRetrieverTitan) {}

object Bio4j {


  def fromS3(logger: Logger, workingDirectory: File, loadingManager: LoadingManager, bio4jLocation: ObjectAddress): Try[Bio4j] = {
    Try {
      logger.info("installing bio4j from location " + bio4jLocation)
      val bio4jDirectory = new File(workingDirectory, "bio4j")
      loadingManager.downloadDirectory(bio4jLocation, bio4jDirectory)
      val bio4jManager = new Bio4jManager(bio4jDirectory.getAbsolutePath)
      new Bio4j (new NodeRetrieverTitan(bio4jManager))
    }
  }
}

