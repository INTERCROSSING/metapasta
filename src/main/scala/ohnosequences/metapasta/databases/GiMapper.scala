package ohnosequences.metapasta.databases

import java.io.File
import ohnosequences.awstools.s3.{LoadingManager, ObjectAddress}
import scala.collection.mutable
import org.clapper.avsl.Logger
import ohnosequences.metapasta.Factory

/**
 * todo it will moved to bio4j
 */
trait GIMapper {
  def getTaxIdByGi(gi: String): Option[String]
}

class InMemoryGIMapperFactory() extends DatabaseFactory[GIMapper] {

  val logger = Logger(this.getClass)

  class InMemoryGIMapper(map: mutable.HashMap[String, String]) extends GIMapper {
    override def getTaxIdByGi(gi: String): Option[String] = map.get(gi)
  }


  override def build(loadingManager: LoadingManager): GIMapper = {
    val mapping = new mutable.HashMap[String, String]()
    val mappingFile = new File("gi.map")
    loadingManager.download(ObjectAddress("metapasta", "gi.map"), mappingFile)
    val giP = """(\d+)\s+(\d+).*""".r
    for(line <- io.Source.fromFile(mappingFile).getLines()) {
      line match {
        case giP(gi, tax) => mapping.put(gi, tax)
        case l => logger.error("can't parse " + l)
      }
    }

    new InMemoryGIMapper(mapping)
  }

}
