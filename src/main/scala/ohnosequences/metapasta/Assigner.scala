package ohnosequences.metapasta

import ohnosequences.logging.Logger

import scala.collection.mutable
import ohnosequences.metapasta.databases.{RawRefId, TaxonRetriever, ReferenceId, Database16S}
import ohnosequences.formats.{RawHeader, FASTQ}



case class ReadId(readId: String)

case class Hit[R <: ReferenceId](readId: ReadId, refId: R, score: Double)


trait AssignerAlgorithm[R <: ReferenceId] {


  def assignAll(taxonomyTree: Tree[Taxon], hits: List[Hit[R]], reads: List[FASTQ[RawHeader]], getTaxIds: (List[Hit[R]], Logger) =>  (List[(Hit[R], Taxon)], Set[R]), logger: Logger): (mutable.HashMap[ReadId, Assignment[R]], mutable.HashSet[R]) = {
    val result = new mutable.HashMap[ReadId, Assignment[R]]()

    val wrongRefIdsAll = new mutable.HashSet[R]()

    val hitsPerReads: Map[ReadId, List[Hit[R]]] = hits.groupBy(_.readId)

    for ( (readId, hits) <- hitsPerReads) {
      val (hitsTaxa, wrongRefIds) = getTaxIds(hits, logger)
      wrongRefIdsAll ++= wrongRefIds
      if (hitsTaxa.isEmpty) {
        result.put(readId, NoTaxIdAssignment(hits.map(_.refId).toSet))
      } else {
        result.put(readId, assign(taxonomyTree, hitsTaxa, logger))
      }

    }
    (result, wrongRefIdsAll)

  }

  def assign(taxonomyTree: Tree[Taxon], hitsTaxa: List[(Hit[R], Taxon)], logger: Logger): Assignment[R]
}

class LCAAlgorithm[R <: ReferenceId](assignmentConfiguration: AssignmentConfiguration) extends AssignerAlgorithm[R] {
  override def assign(taxonomyTree: Tree[Taxon], hitsTaxa: List[(Hit[R], Taxon)], logger: Logger): Assignment[R] = {

    val maxScore = hitsTaxa.map(_._1.score).max

    hitsTaxa.filter { case (hit, taxon) =>
      hit.score >= assignmentConfiguration.bitscoreThreshold &&
      hit.score >= assignmentConfiguration.p * maxScore
    } match {
      case Nil => NotAssigned("threshold", hitsTaxa.map(_._1.refId).toSet, hitsTaxa.map(_._2).toSet)
      case filteredHitsTaxa => {
        val taxa = filteredHitsTaxa.map(_._2).toSet
        val refIds = filteredHitsTaxa.map(_._1.refId).toSet

        TreeUtils.isInLine(taxonomyTree, taxa.toSet) match {
          case Some(specific) => {
            logger.info("taxa form a line: " + taxa.map(_.taxId) + " most specific: " + specific.taxId)
            TaxIdAssignment(specific, hitsTaxa.map(_._1.refId).toSet, line = true)
          }
          case None => {
            //calculating lca
            val lca = TreeUtils.lca(taxonomyTree, taxa)
            logger.info("taxa not in a line: " + taxa.map(_.taxId) + " lca: " + lca.taxId)
            TaxIdAssignment(lca, refIds, lca = true)
          }
        }
      }
    }
  }
}

class BBHAlgorithm[R <: ReferenceId] extends AssignerAlgorithm[R] {
  override def assign(taxonomyTree: Tree[Taxon], hitsTaxa: List[(Hit[R], Taxon)], logger: Logger): Assignment[R] = {
    val (hit, taxon) = hitsTaxa.max(new Ordering[(Hit[R], Taxon)] {
      override def compare(x: (Hit[R], Taxon), y: (Hit[R], Taxon)): Int = (y._1.score - x._1.score).signum
    })
    TaxIdAssignment(taxon, Set(hit.refId), bbh = true)
  }
}

sealed trait Assignment[R <: ReferenceId] {
  type AssignmentCat <: AssignmentCategory
}

case class TaxIdAssignment[R <: ReferenceId](taxon: Taxon, refIds: Set[R], lca: Boolean = false, line: Boolean = false, bbh: Boolean = false) extends Assignment[R] {
  type AssignmentCat = Assigned.type
}

case class NoTaxIdAssignment[R <: ReferenceId](refIds: Set[R]) extends Assignment[R] {
  type AssignmentCat = NoTaxId.type
}

case class NotAssigned[R <: ReferenceId](reason: String, refIds: Set[R], taxIds: Set[Taxon]) extends Assignment[R] {
  type AssignmentCat = NotAssignedCat.type
}



object AssignerAlgorithms {
 // def groupHits(hits: List[Hit], )
}

class Assigner[R <: ReferenceId](taxonomyTree: Tree[Taxon],
               database: Database16S[R],
               taxonRetriever: TaxonRetriever[R],
               extractReadId: String => ReadId,
               assignmentConfiguration: AssignmentConfiguration,
               fastasWriter: Option[FastasWriter]) {

 // val tree: Tree[Taxon] = new Bio4JTaxonomyTree(nodeRetriever)


  def assign(logger: Logger, chunk: ChunkId, reads: List[FASTQ[RawHeader]], hits: List[Hit[R]]):
    (AssignTable, Map[(String, AssignmentType), ReadsStats]) = {



    val (lcaAssignments, lcaWrongRefIds) = logger.benchExecute("LCA assignment") {
      new LCAAlgorithm[R](assignmentConfiguration).assignAll(taxonomyTree, hits, reads, getTaxIds, logger)
    }

    val lcaRes = logger.benchExecute("preparing results") {
      prepareAssignedResults(logger, chunk, LCA, reads, lcaAssignments, lcaWrongRefIds)
    }


    val (bbhAssignments, bbhWrongRefIds) = logger.benchExecute("BBH assignment") {
      new BBHAlgorithm[R].assignAll(taxonomyTree, hits, reads, getTaxIds, logger)
    }

    val bbhRes = logger.benchExecute("preparing results") {
      prepareAssignedResults(logger, chunk, BBH, reads, bbhAssignments, bbhWrongRefIds)
    }

    (assignTableMonoid.mult(lcaRes._1, bbhRes._1), Map((chunk.sample.id, LCA) -> lcaRes._2, (chunk.sample.id, BBH) -> bbhRes._2))

  }

//  def getTaxIdFromRefId(refId: RefId, logger: Logger): Option[Taxon] = {
//
//    database.parseGI(refId.refId) match {
//      case Some(gi) => giMapper.getTaxIdByGi(gi) match {
//        case None => /*logger.error("database error: can't parse taxId from gi: " + refId);*/ None
//        case Some(taxon) => Some(taxon)
//      }
//      case None => {
//        //logger.error("database error: can't parse gi from ref id: " + refId)
//        None
//      }
//    }
//  }

  //
  def getTaxIds(hits: List[Hit[R]], logger: Logger): (List[(Hit[R], Taxon)], Set[R]) = {
    val wrongRefs = new mutable.HashSet[R]()
    val taxons = hits.flatMap { hit =>
      taxonRetriever.getTaxon(hit.refId) match {
        case Some(taxon) if taxonomyTree.isNode(taxon) => Some((hit, taxon))
        case None => {
          logger.warn("couldn't find taxon for ref id: " + hit.refId.id)
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
                             assignments: mutable.HashMap[ReadId, Assignment[R]],
                             wrongRefId: mutable.HashSet[R]): (AssignTable, ReadsStats) = {

    val readsStatsBuilder = new ReadStatsBuilder(wrongRefId.map(_.id))

    reads.foreach {
      fastq =>
        val readId = extractReadId(fastq.header.getRaw)
        assignments.get(readId) match {
          case None => {
            //nohit
            fastasWriter.foreach(_.writeNoHit(fastq, readId, chunk.sample))
            logger.info(readId + " -> " + "no hits")
            readsStatsBuilder.incrementByCategory(NoHit)
          }
          case Some(assignment) => {
            fastasWriter.foreach(_.write(chunk.sample, fastq, readId, assignment))
            logger.info(readId + " -> " + assignment)
            readsStatsBuilder.incrementByAssignment(assignment)
          }
        }
    }

    fastasWriter.foreach(_.uploadFastas(chunk, assignmentType))

    val assignTable = mutable.HashMap[Taxon, TaxInfo]()

    //generate assign table
    assignments.foreach {
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

    (AssignTable(Map((chunk.sample.id, assignmentType) -> assignTable.toMap)), readStats)
  }

}



