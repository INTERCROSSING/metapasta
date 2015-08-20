package ohnosequences.metapasta.databases

import java.io.{FileInputStream, File}
import java.util.zip.GZIPInputStream

import ohnosequences.awstools.s3.{ObjectAddress, LoadingManager}
import ohnosequences.logging.Logger
import ohnosequences.metapasta._

import scala.collection.mutable
import scala.util.Try

class OhnosequencesNCBITaxonomy(
                    nodes: ObjectAddress = ObjectAddress("metapasta", "taxonomy/august.15/nodes.dmp.gz"),
                    names: ObjectAddress = ObjectAddress("metapasta", "taxonomy/august.15/names.dmp.gz")
                    ) extends Installable[Taxonomy] {

  class InMemoryNCBITaxonomy(nodes: Map[Taxon, (Taxon, TaxonomyRank)], names: Map[Taxon, String]) extends Taxonomy {

    override val tree: Tree[Taxon] = new Tree[Taxon] {

      override def getParent(node: Taxon): Option[Taxon] = nodes.get(node) match {
        case Some((ptaxon, rank)) if !ptaxon.equals(node) => Some(ptaxon)
        case _ => None
      }

      override def isNode(node: Taxon): Boolean = {
        nodes.contains(node)
      }

      override val root: Taxon = Taxon("1")

    }

    override def getTaxonInfo(taxon: Taxon): Option[TaxonInfo] = {
      nodes.get(taxon).map { case (rawParent, rank) =>
        TaxonInfo (
          taxon = taxon,
          parentTaxon = if (rawParent.equals(taxon)) None else Some(rawParent),
          scientificName = names.getOrElse(taxon, ""),
          rank = rank
        )
      }
    }
  }

  override def install(logger: Logger, workingDirectory: File, loadingManager: LoadingManager): Try[Taxonomy] = {

    val nodesFile = new File(workingDirectory, "nodes.dmp.gz")
    val namesFile = new File(workingDirectory, "names.dmp.gz")

    logger.info("downloading nodes.dmp.gz from " + nodes.url)
    Try {loadingManager.download(nodes, nodesFile)}.flatMap { r1 =>
      logger.info("downloading names.dmp.gz from " + names.url)
      Try {loadingManager.download(names, namesFile)}.flatMap { r2 =>
        logger.info("loading nodes information from nodes.dmp.gz")
        parseNodes(logger, nodesFile).flatMap { map1 =>
          logger.info("loading nodes names from names.dmp.gz")
          parseNames(logger, namesFile).map { map2 =>
            new InMemoryNCBITaxonomy(map1, map2)
          }
        }
      }
    }
  }

  def parseNodes(logger: Logger, nodesFile: File): Try[Map[Taxon, (Taxon, TaxonomyRank)]] = {
    val res = new mutable.HashMap[Taxon, (Taxon, TaxonomyRank)]()
    Try {
      io.Source.fromInputStream(new GZIPInputStream(new FileInputStream(nodesFile))).getLines().foreach { s =>
        s.split('|').toList match {
          case rawTaxonId :: rawParentTaxonId :: rawRank :: rest => {
            val taxon = Taxon(rawTaxonId.trim)
            val parentTaxon = Taxon(rawParentTaxonId.trim)
            val taxonomyRank = TaxonomyRank(rawRank.trim)
            res.put(taxon, (parentTaxon, taxonomyRank))
          }
          case _ => {
            logger.warn("couldn't parse the line: " + s)
          }
        }
      }
      res.toMap
    }
  }

  def parseNames(logger: Logger, namesFile: File): Try[Map[Taxon, String]] = {
    val res = new mutable.HashMap[Taxon, String]()
    Try {
      io.Source.fromInputStream(new GZIPInputStream(new FileInputStream(namesFile))).getLines().foreach { s =>
        s.split('|').toList match {
          case rawTaxonId :: rawName :: rest => {
            val taxon = Taxon(rawTaxonId.trim)
            val scientificName = rawName.trim
            res.put(taxon, scientificName)
          }
          case _ => {
            logger.warn("couldn't parse the line: " + s)
          }
        }
      }
      res.toMap
    }
  }

}
