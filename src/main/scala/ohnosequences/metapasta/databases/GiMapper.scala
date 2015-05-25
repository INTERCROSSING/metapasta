package ohnosequences.metapasta.databases

import java.io.File
import ohnosequences.awstools.s3.{LoadingManager, ObjectAddress}
import ohnosequences.logging.Logger
import scala.collection.mutable
import ohnosequences.metapasta.{Taxon, Factory}

import scala.util.Try

trait ReferenceId

trait TaxonRetriever[R <: ReferenceId] {
  def getTaxon(referenceId: R): Option[Taxon]
}

case class GI(id: String) extends ReferenceId

object inMemoryGIMapperFactory extends DatabaseFactory[TaxonRetriever[GI]] {


  class InMemoryGIMapper(map: mutable.HashMap[String, Taxon]) extends TaxonRetriever[GI] {
    override def getTaxon(referenceId: GI): Option[Taxon] = map.get(referenceId.id)
  }


  override def build(logger: Logger, loadingManager: LoadingManager): Try[TaxonRetriever[GI]] = {
    Try {
      val mapping = new mutable.HashMap[String, Taxon]()
      val mappingFile = new File("gi.map")
      loadingManager.download(ObjectAddress("metapasta", "gi.map"), mappingFile)
      val giP = """(\d+)\s+(\d+).*""".r
      for (line <- io.Source.fromFile(mappingFile).getLines()) {
        line match {
          case giP(gi, tax) => mapping.put(gi, Taxon(tax))
          case l => logger.error("can't parse " + l)
        }
      }

      new InMemoryGIMapper(mapping)
    }
  }

}
