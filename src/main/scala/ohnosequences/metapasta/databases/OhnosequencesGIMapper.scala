package ohnosequences.metapasta.databases

import java.io.{FileInputStream, File}
import java.util.zip.GZIPInputStream

import ohnosequences.awstools.s3.{LoadingManager, ObjectAddress}
import ohnosequences.logging.Logger
import ohnosequences.metapasta.Taxon

import scala.collection.mutable
import scala.util.Try

/**
 * NCBI GI to taxon id mapper restricted to 16S database
 */
class OhnosequencesGIMapper(giTaxId: ObjectAddress = ObjectAddress("metapasta", "taxonomy/august.15/gi_taxid_nucl.dmp.filtered.gz")) extends Installable[TaxonRetriever[GI]] {

  class InMemoryGIMemoryC(map: Map[String, Taxon]) extends GIMapper {
    override def getTaxon(gi: GI): Option[Taxon] = map.get(gi.id)
  }


  override protected def install(logger: Logger, workingDirectory: File, loadingManager: LoadingManager): Try[GIMapper] = {
    Try {
      val mappingFile = new File(workingDirectory, "gi_taxid.dmp.gz")
      logger.info("downloading gi.map from " + giTaxId.url)
      loadingManager.download(giTaxId, mappingFile)

      logger.info("parsing gi_taxid.dmp.gz")
      val builder = new mutable.HashMap[String, Taxon]()
      io.Source.fromInputStream(new GZIPInputStream(new FileInputStream(mappingFile))).getLines().foreach { s =>
        s.trim.split("\\s+").toList match {
          case gi :: taxonId :: rest => {
            builder.put(gi, Taxon(taxonId))
          }
          case _ => logger.warn("couldn't parse line: " + s)
        }
      }
      new InMemoryGIMemoryC(builder.toMap)
    }
  }

}

