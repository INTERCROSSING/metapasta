package ohnosequences.metapasta

import org.clapper.avsl.Logger
import scala.collection.mutable
import ohnosequences.metapasta.databases.{GIMapper, Database16S}
import scala.collection.mutable.ListBuffer
import ohnosequences.formats.{RawHeader, FASTQ}

case class Hit(readId: String, refId: String, score: Int)
case class Assignment(map: Map[String, String])


//todo track read id!!!!
class Assigner(nodeRetriever: NodeRetriever, database: Database16S, giMapper: GIMapper, paradigm: AssignmentParadigm, extractReadHeader: String => String) {


  val logger = Logger(this.getClass)

  def assign(chunk: MergedSampleChunk, reads: List[FASTQ[RawHeader]], hits: List[Hit]): (AssignTable, (List[ReadInfo], ReadsStats)) = paradigm match {
    case BestHit => assignBestHit(chunk, reads, hits)
    case LCA => assignLCA(chunk, reads, hits)
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

  def getParerntsIds(tax: String): List[String] = {
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

  def assignLCA(chunk: MergedSampleChunk, reads: List[FASTQ[RawHeader]], hits: List[Hit]): (AssignTable, (List[ReadInfo], ReadsStats)) = null

  def assignBestHit(chunk: MergedSampleChunk, reads: List[FASTQ[RawHeader]], hits: List[Hit]): (AssignTable, (List[ReadInfo], ReadsStats)) = {
    val bestHits = mutable.HashMap[String, Hit]()
    hits.foreach {
      hit =>
        if (!bestHits.contains(hit.readId) || bestHits(hit.readId).score < hit.score) {
          //update
          bestHits.put(hit.readId, hit)
        }
    }

    val readsInfo = new ListBuffer[ReadInfo]()

    // prepare reads info

    //readsStats
    var assigned = 0
    var unassigned = 0
    var unknownGI = 0
    var unknownTaxId = 0

    reads.foreach {fastq =>
      val readId = extractReadHeader(fastq.header.getRaw)

      bestHits.get(readId) match {
        case None => {
          readsInfo += ReadInfo(readId, ReadInfo.unassigned, fastq.sequence, fastq.quality, chunk.sample, chunk.chunkId, ReadInfo.unassigned)
          unassigned += 1
        }
        case Some(hit) => {
          //todo add counts here
          val gi = database.parseGI(hit.refId).getOrElse(ReadInfo.unassigned)
          val taxId = giMapper.getTaxIdByGi(gi).getOrElse(ReadInfo.unassigned)
          readsInfo += ReadInfo(readId, gi, fastq.sequence, fastq.quality, chunk.sample, chunk.chunkId, taxId)
          if (gi.equals(ReadInfo.unassigned)) {
            unknownGI += 1
          } else if (taxId.equals(ReadInfo.unassigned)) {
            unknownTaxId += 1
          } else {
            assigned += 1
          }
        }
      }
    }

    val assignTable = mutable.HashMap[String, TaxInfo]()


    //generate reads info
    bestHits.foreach {
      case (readId, hit) =>
        getTaxIdFromRefId(hit.refId) match {
          case None => //todo fix this none
          case Some(taxId) => {
            assignTable.get(taxId) match {
              case None => assignTable.put(taxId, TaxInfo(1, 1))
              case Some(TaxInfo(count, acc)) => assignTable.put(taxId, TaxInfo(count + 1, acc + 1))
            }
            getParerntsIds(taxId).foreach {
              p =>
                assignTable.get(p) match {
                  case None => assignTable.put(p, TaxInfo(0, 1))
                  case Some(TaxInfo(count, acc)) => assignTable.put(p, TaxInfo(count, acc + 1))
                }
            }
          }
        }
    }

    assignTable.put(ReadInfo.unassigned, TaxInfo(unassigned, unassigned))
    val readsStats = ReadsStats(
      assigned = assigned,
      unassigned = unassigned,
      unknownGI = unknownGI,
      unknownTaxId = unknownTaxId,
      unmerged = 0
    )
    (AssignTable(Map(chunk.sample -> assignTable.toMap)), (readsInfo.toList,readsStats))
  }

}
