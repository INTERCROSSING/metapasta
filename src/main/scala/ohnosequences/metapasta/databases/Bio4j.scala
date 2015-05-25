package ohnosequences.metapasta

import java.io.File

import ohnosequences.awstools.s3.{LoadingManager, S3, ObjectAddress}
import ohnosequences.logging.Logger
import ohnosequences.metapasta.databases.DatabaseFactory

import scala.util.Try

trait Bio4j {
  val nodeRetriever: com.ohnosequences.bio4j.titan.model.util.NodeRetrieverTitan
}

class Bio4jFactory(bio4jLocation: ObjectAddress, dbDirectory: File) extends DatabaseFactory[Bio4j] {


  class BundleNodeRetriever extends Bio4j {
    var nodeRetriever = ohnosequences.bio4j.bundles.NCBITaxonomyDistribution.nodeRetriever
  }

  override def build(logger: Logger, loadingManager: LoadingManager): Try[Bio4j] = {
    Try {
      logger.info("installing bio4j from location " + bio4jLocation)

      if (!dbDirectory.exists) {
        dbDirectory.mkdirs
      }

      loadingManager.downloadDirectory(bio4jLocation, dbDirectory)

      val bio4jManager = new Bio4jManager(dbDirectory.getAbsolutePath)

      new Bio4j {
        override val nodeRetriever = new NodeRetrieverTitan(bio4jManager)
      }

    }
  }
}

