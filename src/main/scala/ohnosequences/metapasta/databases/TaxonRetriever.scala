package ohnosequences.metapasta.databases

import java.io.File

import ohnosequences.awstools.s3.{LoadingManager, ObjectAddress}
import ohnosequences.logging.Logger
import ohnosequences.metapasta.Taxon

import scala.collection.mutable
import scala.util.Try

trait ReferenceId {
  val id: String

  def toRaw = RawRefId(id)
}

//for results
case class RawRefId(id: String) extends ReferenceId

trait TaxonRetriever[R <: ReferenceId] {
  def getTaxon(referenceId: R): Option[Taxon]
}

case class GI(id: String) extends ReferenceId


class InMemoryGIMapper(map: mutable.HashMap[String, Taxon]) extends TaxonRetriever[GI] {
  override def getTaxon(referenceId: GI): Option[Taxon] = map.get(referenceId.id)
}

object TaxonRetriever {

  object inMemory extends Installable[TaxonRetriever[GI]] {
    val s3location = ObjectAddress("metapasta", "gi.map")

    override def install(logger: Logger, workingDirectory: File, loadingManager: LoadingManager): Try[TaxonRetriever[GI]] = {
      Try {
        val mapping = new mutable.HashMap[String, Taxon]()
        workingDirectory.mkdir()
        val mappingFile = new File(workingDirectory, "gi.map")
        if (!mappingFile.exists()) {
          loadingManager.download(s3location, mappingFile)
        }
        val giP = """(\d+)\s+(\d+).*""".r
        for (line <- scala.io.Source.fromFile(mappingFile).getLines()) {
          line match {
            case giP(gi, tax) => mapping.put(gi, Taxon(tax))
            case l => logger.error("can't parse " + l)
          }
        }
        new InMemoryGIMapper(mapping)
      }
    }
  }


}
