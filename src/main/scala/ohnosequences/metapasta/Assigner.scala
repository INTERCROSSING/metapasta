package ohnosequences.metapasta

import org.clapper.avsl.Logger
import scala.collection.mutable
import ohnosequences.metapasta.databases.{GIMapper, Database16S}
import scala.collection.mutable.ListBuffer
import ohnosequences.formats.{RawHeader, FASTQ}

case class Hit(readId: String, refId: String, score: Int)

trait Assignment {
  def score: Int
}
case class TaxIdAssignment(taxId: String, score: Int = 0) extends Assignment
case class RefIdAssignment(refId: String, score: Int = 0) extends Assignment


//todo track read id!!!!
class Assigner(nodeRetriever: NodeRetriever, database: Database16S, giMapper: GIMapper, paradigm: AssignmentParadigm, extractReadHeader: String => String) {


  val logger = Logger(this.getClass)

  def assign(chunk: MergedSampleChunk, reads: List[FASTQ[RawHeader]], hits: List[Hit]): (AssignTable, (List[ReadInfo], ReadsStats)) = paradigm match {
    case BestHit => assignBestHit(chunk, reads, hits)
    case LCA(treshold) => assignLCA(chunk, reads, hits, treshold)
  }

  def getTaxIdFromRefId(refId: String): Option[String] = {
    database.parseGI(refId) match {
      case Some(gi) => giMapper.getTaxIdByGi(gi) match {
        case None => logger.error("database error: can't parse taxId from gi: " + refId); None
        case Some(taxId) => Some(taxId)
      }
      case None => {
        logger.error("database error: can't parse gi from ref id: " + refId)
        None
      }
    }
  }

  def getParentsIds(tax: String): List[String] = {
    val res = mutable.ListBuffer[String]()
    val node = nodeRetriever.nodeRetriever.getNCBITaxonByTaxId(tax)
    if(node==null) {
      logger.error("can't receive node for " + tax)
    } else {
      var parent = node.getParent()
      while (parent != null) {
        res += parent.getTaxId()
        //println(parent.getTaxId())
        parent = parent.getParent()
      }
    }
    // println(res.size)
    res.toList
  }
  
  def prepareAssignedResults(chunk: MergedSampleChunk,
                             reads: List[FASTQ[RawHeader]],
                             assignment: mutable.HashMap[String, Assignment],
                             initialReadsStats: ReadsStats = readsStatsMonoid.unit): (AssignTable, (List[ReadInfo], ReadsStats)) = {
    
    val readsInfo = new ListBuffer[ReadInfo]()

    // prepare reads info

    //readsStats
    val readsStatsBuilder = new ReadsStatsBuilder()

    reads.foreach {fastq =>
      val readId = extractReadHeader(fastq.header.getRaw)

      assignment.get(readId) match {
        case None => {
          readsInfo += ReadInfo(readId, ReadInfo.unassigned, fastq.sequence, fastq.quality, chunk.sample, chunk.chunkId, ReadInfo.unassigned)
          readsStatsBuilder.incrementUnassigned()
        }
        case Some(RefIdAssignment(refId, score)) => {
          val gi = database.parseGI(refId).getOrElse(ReadInfo.unassigned)
          val taxId = giMapper.getTaxIdByGi(gi).getOrElse(ReadInfo.unassigned)
          readsInfo += ReadInfo(readId, gi, fastq.sequence, fastq.quality, chunk.sample, chunk.chunkId, taxId)
          if (gi.equals(ReadInfo.unassigned)) {
            readsStatsBuilder.incrementUnknownRefId()
          } else if (taxId.equals(ReadInfo.unassigned)) {
            readsStatsBuilder.incrementUnknownGI()
          } else {
            readsStatsBuilder.incrementAssigned()
          }
        }
        case Some(TaxIdAssignment(taxId, score)) => {
          readsInfo += ReadInfo(readId, ReadInfo.unassigned, fastq.sequence, fastq.quality, chunk.sample, chunk.chunkId, taxId)
          readsStatsBuilder.incrementAssigned()
        }
      }
    }

    val assignTable = mutable.HashMap[String, TaxInfo]()
    //generate reads info
    assignment.foreach {
      case (readId, RefIdAssignment(refId, score)) =>
        getTaxIdFromRefId(refId) match {
          case None => //todo fix this none
          case Some(taxId) => {
            assignTable.get(taxId) match {
              case None => assignTable.put(taxId, TaxInfo(1, 1))
              case Some(TaxInfo(count, acc)) => assignTable.put(taxId, TaxInfo(count + 1, acc + 1))
            }
            getParentsIds(taxId).foreach {
              p =>
                assignTable.get(p) match {
                  case None => assignTable.put(p, TaxInfo(0, 1))
                  case Some(TaxInfo(count, acc)) => assignTable.put(p, TaxInfo(count, acc + 1))
                }
            }
          }
        }
      case (readId, TaxIdAssignment(taxId, score)) =>
            assignTable.get(taxId) match {
              case None => assignTable.put(taxId, TaxInfo(1, 1))
              case Some(TaxInfo(count, acc)) => assignTable.put(taxId, TaxInfo(count + 1, acc + 1))
            }
            getParentsIds(taxId).foreach {
              p =>
                assignTable.get(p) match {
                  case None => assignTable.put(p, TaxInfo(0, 1))
                  case Some(TaxInfo(count, acc)) => assignTable.put(p, TaxInfo(count, acc + 1))
                }
            }
    }

   // assignTable.put(ReadInfo.unassigned, TaxInfo(unassigned, unassigned))

    (AssignTable(Map(chunk.sample -> assignTable.toMap)), (readsInfo.toList,  readsStatsBuilder.build.mult(initialReadsStats)))
  }

  def assignLCA(chunk: MergedSampleChunk, reads: List[FASTQ[RawHeader]], hits: List[Hit], threshold: Double) = {

    val readsStatsBuilder = new ReadsStatsBuilder()

    //ref ids
    val hitsPerReads =  mutable.HashMap[String, mutable.HashSet[String]]()
    hits.foreach {
      hit =>
       // val taxId = getTaxIdFromRefId(hit)
        hitsPerReads.get(hit.readId) match {
        case None => hitsPerReads.put(hit.readId, mutable.HashSet[String](hit.refId))
        case Some(listBuffer) => listBuffer += hit.refId
      }
    }

    val finalHits =  mutable.HashMap[String, Assignment]()

    for( (readId, refIds) <- hitsPerReads) {

      logger.info("refsIds.size=" + refIds.size)
      val counts = mutable.HashMap[String, Int]()
      var total = 0



      var unknownRefId = true
      var unknownGI = true

      for (refId <- refIds) {
        (database.parseGI(refId) match {
          case Some(gi) =>
            unknownRefId = false
            giMapper.getTaxIdByGi(gi) match {
           // case None => logger.error("database error: can't parse taxId from gi: " + refId); None
              case Some(taxId) => unknownGI = false; Some(taxId)
            }
          case None => {
           // logger.error("database error: can't parse gi from ref id: " + refId)
            None
          }
        }).foreach { taxId =>
          total += 1
          for (parTaxId <- getParentsIds(taxId)) {
            val curVal = counts.getOrElse(parTaxId, 0)
            counts.put(parTaxId, curVal + 1)
          }
          val curVal = counts.getOrElse(taxId, 0)
          counts.put(taxId, curVal + 1)
        }
      }

      val t = total * threshold
      var min = total
      val argMins = mutable.HashSet[String]()

      for( (taxId, count) <- counts) {
        if (count > t) {
          if (count < min) {
            argMins.clear()
            argMins += taxId
            min = count
          } else if (count == min) {
            argMins += taxId
          }
        }
      }
      findSpecific(argMins) match {
        case Some(taxId) => finalHits.put(readId, TaxIdAssignment(taxId))
        case None => {
          logger.error("can't find specific! argMins.size=" + argMins.size)
          if (unknownRefId) {
            readsStatsBuilder.incrementUnknownRefId()
          } else if (unknownGI) {
            readsStatsBuilder.incrementUnknownGI()
          } else {
            readsStatsBuilder.incrementLCAFiltered()
          }
        }
      }
    }    
    prepareAssignedResults(chunk, reads, finalHits, readsStatsBuilder.build)
  }



  //todo speed up it!
  def findSpecific(ids: mutable.HashSet[String]): Option[String] = {
    ids.find { id1 =>
      ids.forall { id2 =>
        val node2 = nodeRetriever.nodeRetriever.getNCBITaxonByTaxId(id2)
        val p2 = node2.getParent()
        if(p2 == null) {
          true
        } else {
          !p2.getTaxId().equals(id1)
        }
      }
    }
  }

  def assignBestHit(chunk: MergedSampleChunk, reads: List[FASTQ[RawHeader]], hits: List[Hit]): (AssignTable, (List[ReadInfo], ReadsStats)) = {
    val bestHits = mutable.HashMap[String, Assignment]()
    hits.foreach {
      hit =>
        if (!bestHits.contains(hit.readId) || bestHits(hit.readId).score < hit.score) {
          //update
          bestHits.put(hit.readId, RefIdAssignment(hit.refId, hit.score))
        }
    }

    prepareAssignedResults(chunk, reads, bestHits)
  }

}
