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

case class TaxIdAssignment(taxon: Taxon, refIds: Set[RefId], lca: Boolean = false, line: Boolean = false) extends Assignment {
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
  
  def filterHit(hit: Hit, bestScores: mutable.HashMap[ReadId, Double], assignmentConfiguration: AssignmentConfiguration): Boolean = {
    val bestScore = bestScores.getOrElse(hit.readId, 0D)
    if (hit.score >= math.max(assignmentConfiguration.bitscoreThreshold, assignmentConfiguration.p * bestScore)) {
      true
    } else {
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

  def getTaxIdFromRefId(logger: Logger, refId: RefId): Option[Taxon] = {
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
      case (readId, TaxIdAssignment(taxon, refIds, _, _)) =>
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

    val hitsPerReads = mutable.HashMap[ReadId, mutable.HashSet[RefId]]()

    val bestScores = AssignerAlgorithms.bestScores(hits)

    //filter hits
    hits.foreach { hit =>
      val refs = hitsPerReads.get(hit.readId) match {
        case None => {
          val refsSet = new mutable.HashSet[RefId]()
          hitsPerReads.put(hit.readId, refsSet)
          refsSet
        }
        case Some(refsIds) => refsIds
      }

      if (AssignerAlgorithms.filterHit(hit, bestScores, assignmentConfiguration)) {
        refs += hit.refId
      } else {
        val bestScore = bestScores.getOrElse(hit.readId, 0D)
        logger.warn("hit " + hit + " has been filtered; best score for read id is " + bestScore)
      }
    }

    //now hitsPerReads contains empty hash set for reads with filtered hits
    val finalAssignments = mutable.HashMap[ReadId, Assignment]()
    val readsStatsBuilder = new ReadStatsBuilder()

    for ((readId, refIds) <- hitsPerReads) {

      //we will need thsi mapping to get all ref ids
      val refid2taxon = new mutable.HashMap[RefId, Taxon]()
      for (refId <- refIds) {
        getTaxIdFromRefId(logger, refId) match {
          case Some(taxon) if taxonomyTree.isNode(taxon) => refid2taxon += ((refId, taxon))
          case None => {
            logger.warn("couldn't find taxon for ref id: " + refId)
            readsStatsBuilder.addWrongRefId(refId)
          }
          case Some(taxId) => {
            logger.warn("taxon with id: " + taxId + " is not presented in " + database.name)
            readsStatsBuilder.addWrongRefId(refId)
          }
        }
      }

      val taxa: Set[Taxon] = refid2taxon.values.toSet

      //now there are four cases:
      //* we had some not filtered hits, but all of them have wrong gi - NoTaxIdAssignment
      //* we have empty ref ids that means that all hits were filtered -  NotAssigned
      // * rest hits form a line in the taxonomy tree. In this case we should choose the most specific tax id
      // * in other cases we should calculate LCA
      val assignment = if (taxa.isEmpty && !refIds.isEmpty) {
        //couldn't get taxa from any of ref id
        NoTaxIdAssignment(refIds.toSet)
      } else if (refIds.isEmpty) {
        //nothing to assign
        NotAssigned("hits were filtered", refIds.toSet, taxa)
      } else {
        TreeUtils.isInLine(taxonomyTree, taxa) match {
          case Some(specific) => {
            logger.info("taxa form a line: " + taxa)
            logger.info("the most specific taxon: " + specific)
            val specificRefIds = refid2taxon.filter(_._2.equals(specific)).keys.toSet
            TaxIdAssignment(specific, specificRefIds, line = true)
          }
          case None => {
            //calculating lca
            val lca = TreeUtils.lca(taxonomyTree, taxa.toList)
            TaxIdAssignment(lca, refIds.toSet, lca = true)
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
      getTaxIdFromRefId(logger, hit.refId) match {
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
              assignment.put(hit.readId, TaxIdAssignment(taxon, Set(hit.refId)))
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



