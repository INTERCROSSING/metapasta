package ohnosequences.metapasta

import scala.collection.mutable
import ohnosequences.metapasta.databases.{GIMapper, Database16S}
import ohnosequences.formats.{RawHeader, FASTQ}
import ohnosequences.nisperon.{AWS, MapMonoid}
import ohnosequences.awstools.s3.ObjectAddress
import ohnosequences.nisperon.logging.{Logger, S3Logger}
import ohnosequences.metapasta.reporting.SampleId
import scala.collection.mutable.ListBuffer

case class ReadId(readId: String)
case class RefId(refId: String)
case class Hit(readId: ReadId, refId: RefId, score: Double)

sealed trait Assignment {
  type AssignmentCat <: AssignmentCategory
}

case class TaxIdAssignment(taxon: Taxon, refIds: Set[RefId], lca: Boolean = false, line: Boolean = false, bbh: Boolean = false) extends Assignment {
  type AssignmentCat = Assigned.type
}

case class NoTaxIdAssignment(refIds: Set[RefId]) extends Assignment {
  type AssignmentCat = NoTaxId.type
}

case class NotAssigned(reason: String, refIds: Set[RefId], taxIds: Set[Taxon]) extends Assignment {
  type AssignmentCat = NotAssignedCat.type
}



object AssignerAlgorithms {


  def bestScores(hits: List[Hit]): mutable.HashMap[ReadId, Double] = {
    val bestScores = mutable.HashMap[ReadId, Double]()
    for (hit <- hits) {
      if (hit.score >= bestScores.getOrElse(hit.readId, 0D)) {
        bestScores.put(hit.readId, hit.score)
      }
    }
    bestScores
  }
  
  def filterHit(hit: Hit, bestScores: mutable.HashMap[ReadId, Double], assignmentConfiguration: AssignmentConfiguration, logger: Logger): Boolean = {
    val bestScore = bestScores.getOrElse(hit.readId, 0D)
    if (hit.score >= math.max(assignmentConfiguration.bitscoreThreshold, assignmentConfiguration.p * bestScore)) {
      true
    } else {
      logger.warn("hit " + hit + " has been filtered; best score for read id is " + bestScore)
      false
    }
  }



 // def groupHits(hits: List[Hit], )
}

class Assigner(taxonomyTree: Tree[Taxon],
               database: Database16S,
               giMapper: GIMapper,
               assignmentConfiguration: AssignmentConfiguration,
               extractReadHeader: String => String,
               fastasWriter: Option[FastasWriter]) {

 // val tree: Tree[Taxon] = new Bio4JTaxonomyTree(nodeRetriever)


  def assign(logger: Logger, chunk: ChunkId, reads: List[FASTQ[RawHeader]], hits: List[Hit]):
    (AssignTable, Map[(String, AssignmentType), ReadsStats]) = {



    val (lcaAssignments, lcaStats) = logger.benchExecute("LCA assignment") {
      assignLCA(logger, chunk, reads, hits)
    }


    val lcaRes = logger.benchExecute("preparing results") {
      prepareAssignedResults(logger, chunk, LCA, reads, lcaAssignments, lcaStats)
    }


    val (bbhAssignments, bbhStats) = logger.benchExecute("BBH assignment") {
      assignBestHit(logger, chunk, reads, hits)
    }

    val bbhRes = logger.benchExecute("preparing results") {
      prepareAssignedResults(logger, chunk, BBH, reads, bbhAssignments, bbhStats)
    }



    (assignTableMonoid.mult(lcaRes._1, bbhRes._1), Map((chunk.sample.id, LCA) -> lcaRes._2, (chunk.sample.id, BBH) -> bbhRes._2))


  }

  def getTaxIdFromRefId(refId: RefId, logger: Logger): Option[Taxon] = {
    database.parseGI(refId.refId) match {
      case Some(gi) => giMapper.getTaxIdByGi(gi) match {
        case None => /*logger.error("database error: can't parse taxId from gi: " + refId);*/ None
        case Some(taxon) => Some(taxon)
      }
      case None => {
        //logger.error("database error: can't parse gi from ref id: " + refId)
        None
      }
    }
  }

  //
  def getTaxIds(hits: List[Hit], logger: Logger): (List[(RefId, Taxon)], Set[RefId]) = {
    val wrongRefs = new mutable.HashSet[RefId]()
    val taxons = hits.flatMap { hit =>
      getTaxIdFromRefId(hit.refId, logger) match {
        case Some(taxon) if taxonomyTree.isNode(taxon) => Some(hit.refId, taxon)
        case None => {
          logger.warn("couldn't find taxon for ref id: " + hit.refId.refId)
          wrongRefs += hit.refId
          None
        }
        case Some(taxon) => {
          logger.warn("taxon with id: " + taxon.taxId + " is not presented in " + database.name)
          wrongRefs += hit.refId
          None
        }
      }
    }
    (taxons, wrongRefs.toSet)
  }


  def prepareAssignedResults(logger: Logger, chunk: ChunkId,
                             assignmentType: AssignmentType,
                             reads: List[FASTQ[RawHeader]],
                             assignment: mutable.HashMap[ReadId, Assignment],
                             initialReadsStats: ReadsStats = readsStatsMonoid.unit): (AssignTable, ReadsStats) = {

    val readsStatsBuilder = new ReadStatsBuilder()

    reads.foreach {
      fastq =>
        val readId = ReadId(extractReadHeader(fastq.header.getRaw))
        assignment.get(readId) match {
          case None => {
            //nohit
            fastasWriter.foreach(_.writeNoHit(fastq, readId))
            logger.info(readId + " -> " + "no hits")
            readsStatsBuilder.incrementByCategory(NoHit)
          }
          case Some(assignment1) => {
            fastasWriter.foreach(_.write(chunk.sample, fastq, readId, assignment1))
            logger.info(readId + " -> " + assignment1)
            readsStatsBuilder.incrementByAssignment(assignment1)
          }
        }
    }

    fastasWriter.foreach(_.uploadFastas(chunk, assignmentType))

    val assignTable = mutable.HashMap[Taxon, TaxInfo]()

    //generate assign table
    assignment.foreach {
      case (readId, TaxIdAssignment(taxon, refIds, _, _, _)) =>
        assignTable.get(taxon) match {
          case None => assignTable.put(taxon, TaxInfo(1, 1))
          case Some(TaxInfo(count, acc)) => assignTable.put(taxon, TaxInfo(count + 1, acc + 1))
        }
        TreeUtils.getLineageExclusive(taxonomyTree, taxon).foreach { p  =>
            assignTable.get(p) match {
              case None => assignTable.put(p, TaxInfo(0, 1))
              case Some(TaxInfo(count, acc)) => assignTable.put(p, TaxInfo(count, acc + 1))
            }
        }
      case _ => ()
    }

    val readStats = readsStatsBuilder.build

    assignTable.put(NoHit.taxon, TaxInfo(readStats.noHit, readStats.noHit))
    assignTable.put(NoTaxId.taxon, TaxInfo(readStats.noTaxId, readStats.noTaxId))
    assignTable.put(NotAssignedCat.taxon, TaxInfo(readStats.notAssigned, readStats.notAssigned))

    (AssignTable(Map((chunk.sample.id, assignmentType) -> assignTable.toMap)), readStats.mult(initialReadsStats))
  }

  def assignLCA(logger: Logger, chunk: ChunkId, reads: List[FASTQ[RawHeader]], hits: List[Hit]): (mutable.HashMap[ReadId, Assignment], ReadsStats) = {

    val bestScores = AssignerAlgorithms.bestScores(hits)

    val hitsPerReads: Map[ReadId, List[Hit]] = hits.groupBy(_.readId)

    val finalAssignments = mutable.HashMap[ReadId, Assignment]()
    val readsStatsBuilder = new ReadStatsBuilder()

    for ((readId, hits) <- hitsPerReads) {

      val filteredHits  = hits.filter(AssignerAlgorithms.filterHit(_, bestScores, assignmentConfiguration, logger))
      val (taxa, wrongRefIds) = getTaxIds(filteredHits, logger)
      wrongRefIds.foreach { refId => readsStatsBuilder.addWrongRefId(refId)}
      val refIds = taxa.map(_._1).toSet

      //now there are four cases:
      //* we had some not filtered hits, but all of them have wrong gi - NoTaxIdAssignment
      //* we have empty ref ids that means that all hits were filtered -  NotAssigned
      // * rest hits form a line in the taxonomy tree. In this case we should choose the most specific tax id
      // * in other cases we should calculate LCA
      val assignment = if (filteredHits.isEmpty) {
        NotAssigned("hits were filtered", hits.map(_.refId).toSet, Set[Taxon]())
      } else if (taxa.isEmpty && filteredHits.nonEmpty) {
        //couldn't get taxa from any of ref id
        NoTaxIdAssignment(filteredHits.map(_.refId).toSet)
      } else {
        val taxaSet: Set[Taxon] = taxa.map{_._2}.toSet
        TreeUtils.isInLine(taxonomyTree, taxaSet) match {
          case Some(specific) => {
            logger.info("taxa form a line: " + taxa)
            logger.info("the most specific taxon: " + specific)
            TaxIdAssignment(specific, refIds, line = true)
          }
          case None => {
            //calculating lca
            val lca = TreeUtils.lca(taxonomyTree, taxaSet)
            TaxIdAssignment(lca, refIds, lca = true)
          }
        }
      }
      finalAssignments.put(readId, assignment)
    }
    (finalAssignments, readsStatsBuilder.build)
  }


  def assignBestHit(logger: Logger, chunk: ChunkId, reads: List[FASTQ[RawHeader]], hits: List[Hit]): (mutable.HashMap[ReadId, Assignment], ReadsStats) = {

    val bestScores = AssignerAlgorithms.bestScores(hits)
    val assignment = mutable.HashMap[ReadId, Assignment]()

    val readsStatsBuilder = new ReadStatsBuilder()

    for (hit <- hits) {
      getTaxIdFromRefId(hit.refId, logger) match {
        case None => {
          readsStatsBuilder.addWrongRefId(hit.refId)
          logger.warn("couldn't find taxon for ref id: " + hit.refId)
          assignment.put(hit.readId, NoTaxIdAssignment(Set(hit.refId)))
        }
        case Some(taxon) => {
          if (taxonomyTree.isNode(taxon)) {
            val bestScore = bestScores.getOrElse(hit.readId, 0D)
            if (hit.score >= bestScore) {
              bestScores.put(hit.readId, hit.score)
              assignment.put(hit.readId, TaxIdAssignment(taxon, Set(hit.refId), bbh = true))
            } else {
              logger.warn("hit " + hit + " has been filtered; best score for read id is " + bestScore)
            }
          } else {
            logger.warn("taxon " + taxon + " is not presented in " + database.name)
            assignment.put(hit.readId, NoTaxIdAssignment(Set(hit.refId)))
            readsStatsBuilder.addWrongRefId(hit.refId)
          }
        }

      }
    }

    (assignment, readsStatsBuilder.build)

  }

}



