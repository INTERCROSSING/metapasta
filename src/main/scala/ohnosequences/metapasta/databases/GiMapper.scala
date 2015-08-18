package ohnosequences.metapasta.databases

import java.util.zip.GZIPInputStream

import ohnosequences.logging.Logger
import ohnosequences.metapasta.Taxon
import ohnosequences.awstools.s3.{LoadingManager, ObjectAddress}

import scala.collection.mutable

import java.io.{FileInputStream, File}

import scala.util.Try

trait GIMapper {
  def getTaxIdByGi(gi: String): Option[Taxon]
}


class InMemoryGIMemory(giTaxId: ObjectAddress = ObjectAddress("metapasta", "0.9.13/gi_taxid_nucl.dmp.filtered.gz")) extends Installable[GIMapper] {

  class InMemoryGIMemoryC(map: Map[String, Taxon]) extends GIMapper {
    override def getTaxIdByGi(gi: String): Option[Taxon] = map.get(gi)
  }


  override protected def install(logger: Logger, workingDirectory: File, loadingManager: LoadingManager): Try[GIMapper] = {
    Try {
      val mappingFile = new File(workingDirectory, "gi.map")
      logger.info("downloading gi.map from " + giTaxId.url)
      loadingManager.download(giTaxId, mappingFile)

      logger.info("parsing gi.map")
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
