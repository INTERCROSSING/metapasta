package ohnosequences.metapasta

import org.clapper.avsl.Logger
import scala.collection.mutable
import ohnosequences.metapasta.databases.{GIMapper, Database16S}
import scala.collection.mutable.ListBuffer
import ohnosequences.formats.{RawHeader, FASTQ}
import java.util
import ohnosequences.nisperon.MapMonoid

case class Hit(readId: String, refId: String, score: Int)

case class Assignment(taxId: String, score: Int = 0)

//trait Assignment {
//  def score: Int
//}
//case class TaxIdAssignment(taxId: String, score: Int = 0) extends Assignment
//case class RefIdAssignment(refId: String, score: Int = 0) extends Assignment


//todo track read id!!!!
class Assigner(nodeRetriever: NodeRetriever, database: Database16S, giMapper: GIMapper, assignmentConfiguration: AssignmentConfiguration, extractReadHeader: String => String) {


  val logger = Logger(this.getClass)

  def assign(chunk: MergedSampleChunk, reads: List[FASTQ[RawHeader]], hits: List[Hit]): (AssignTable, Map[(String, String), ReadsStats]) = {
   // case BestHit => assignBestHit(chunk, reads, hits)
   // case LCA(scoreThreshold, p) => assignLCA(chunk, reads, hits, scoreThreshold, p)

    //logger.info("lca")
    val lcaRes  = assignLCA(chunk, reads, hits, assignmentConfiguration.bitscoreThreshold, assignmentConfiguration.p)

    //logger.info("best score hit")
    val bbhRes  = assignBestHit(chunk, reads, hits)

    import AssignmentType._

    (assignTableMonoid.mult(lcaRes._1, bbhRes._1), Map((chunk.sample,LCA) -> lcaRes._2, (chunk.sample,BBH) -> bbhRes._2))


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

//  todo reads info rejected but should generate fasta files now!!!
//  def prepareAssignedResults(chunk: MergedSampleChunk,
//                             reads: List[FASTQ[RawHeader]],
//                             assignment: mutable.HashMap[String, Assignment],
//                             initialReadsStats: ReadsStats = readsStatsMonoid.unit): (AssignTable, (List[ReadInfo], ReadsStats)) = {
  def prepareAssignedResults(chunk: MergedSampleChunk,
                             assignmentType: AssignmentType,
                             reads: List[FASTQ[RawHeader]],
                             bestScores: mutable.HashMap[String, Int],
                             assignment: mutable.HashMap[String, Assignment],
                             initialReadsStats: ReadsStats = readsStatsMonoid.unit): (AssignTable, ReadsStats) = {
    
   // val readsInfo = new ListBuffer[ReadInfo]()

    // prepare reads info

    //readsStats
    val readsStatsBuilder = new ReadsStatsBuilder()

    reads.foreach {
      fastq =>
       // readsStatsBuilder.incrementMerged()
        //todo check it
        val readId = extractReadHeader(fastq.header.getRaw)
        bestScores.get(readId) match {
          case None => readsStatsBuilder.incrementNoHit()
          case _ => assignment.get(readId) match {
            case None => readsStatsBuilder.incrementNotAssigned()
            case _ => readsStatsBuilder.incrementAssigned()
          }
        }
    }


    val assignTable = mutable.HashMap[String, TaxInfo]()

    //generate assign table
    assignment.foreach {
      case (readId, Assignment(taxId, score)) =>
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

    (AssignTable(Map((chunk.sample, assignmentType.toString) -> assignTable.toMap)), readsStatsBuilder.build.mult(initialReadsStats))
  }

  def assignLCA(chunk: MergedSampleChunk, reads: List[FASTQ[RawHeader]], hits: List[Hit], scoreThreshold: Int, p: Double): (AssignTable, ReadsStats) = {

    logger.info("LCA assignment")
    var t1 =  System.currentTimeMillis()

    val readsStatsBuilder = new ReadsStatsBuilder()

    //ref ids!!!!
    val hitsPerReads =  mutable.HashMap[String, mutable.HashSet[String]]()

    //reads to best scores
    val bestScores = mutable.HashMap[String, Int]()

    //find best scores
    for( hit <- hits) {
      if(hit.score >= bestScores.getOrElse(hit.readId, 0)) {
        bestScores.put(hit.readId, hit.score)
      }
    }

    //now we can now whe we have no hits



    hits.foreach {
      //filter all reads with bitscore below p * S (where p is fixed coefficient, e.g. 0.9)
      case hit if hit.score >= math.max(scoreThreshold, p * bestScores.getOrElse(hit.readId, 0)) => {
       // val taxId = getTaxIdFromRefId(hit)
        hitsPerReads.get(hit.readId) match {
          case None => hitsPerReads.put(hit.readId, mutable.HashSet[String](hit.refId))
          case Some(listBuffer) => listBuffer += hit.refId
        }
      }
      case _ => ()
    }



    val finalHits =  mutable.HashMap[String, Assignment]()

    for( (readId, refIds) <- hitsPerReads) {

      //logger.info("refsIds.size=" + refIds.size)

      // 1. map ref ids to

      val taxIds = new mutable.HashSet[String]()

      for (refId <- refIds) {
        getTaxIdFromRefId(refId) match {
          case Some(taxId) => taxIds.add(taxId)
          case None => readsStatsBuilder.addWrongRefId(refId)
        }
      }


      //now there are two cases:

      // * rest hits form a line in the taxonomy tree. In this case we should choose the most specific tax id
      // * in other cases we should calculate LCA

      isInLine(taxIds) match {
        case Some(specific) => {
          finalHits.put(readId, Assignment(specific))
        }
        case None => {
          //lca
          if (taxIds.isEmpty) {
            //nothing to assign
          } else {
            var r: Option[String] = None
            for (taxId <- taxIds) {
              r = r.flatMap(lca(_, taxId)) match {
                case None => logger.error("can't calculate lca(" + r + ", " + taxId + ")"); None
                case Some(rr) => Some(rr)
              }
            }
            r match {
              case None => //no assigment
              case Some(rr) => finalHits.put(readId, Assignment(rr))
            }
          }
        }
      }
    }

    var t2 = System.currentTimeMillis()
    logger.info("LCA assignment finished " + (t2 - t1)  + " ms")


    logger.info("preparing results")
    t1 = System.currentTimeMillis()
      //generate stats
    val res = prepareAssignedResults(chunk, LCA, reads, bestScores, finalHits, readsStatsBuilder.build)
    t2 = System.currentTimeMillis()
    logger.info("preparing results finished " + (t2 - t1)  + " ms")

    res
  }


  //return most specific one if true

  //most specific is one that have longest parent line
  def isInLine(taxIds: mutable.HashSet[String]): Option[String] = {
    if (taxIds.isEmpty) {
      None
    } else if (taxIds.size == 1) {
      Some(taxIds.head)
    } else {
      var max = 0
      var argmax = List[String]()

      for(taxId <- taxIds) {
        val c = getParentsIds(taxId)
        if(c.size > max) {
          max = c.size
          argmax = c
        }
      }
      //first taxIds.size elements should be taxIds

      if (argmax.take(taxIds.size).forall(taxIds.contains)) {
        argmax.headOption
      } else {
        None
      }
    }
  }

  //todo  super slow
  def lca(tax1: String, tax2: String): Option[String] =  {
    if(tax1.equals(tax2)) {
      Some(tax1)
    } else {
      val par1 = getParentsIds(tax1)
      val par2 = getParentsIds(tax2)
      var r = ""

      var p1 = par1.size -1
      var p2 = par2.size -1

      while (par1(p1).equals(par2(p2))) {
        r = par1(p1)
        p1 -= 1
        p2 -= 1
      }

      if (r.isEmpty) None else Some(r)
    }
  }

  //todo speed up it!
//  def findSpecific(ids: mutable.HashSet[String]): Option[String] = {
//    ids.find { id1 =>
//      ids.forall { id2 =>
//        val node2 = nodeRetriever.nodeRetriever.getNCBITaxonByTaxId(id2)
//        val p2 = node2.getParent()
//        if(p2 == null) {
//          true
//        } else {
//          !p2.getTaxId().equals(id1)
//        }
//      }
//    }
//  }

  def assignBestHit(chunk: MergedSampleChunk, reads: List[FASTQ[RawHeader]], hits: List[Hit]): (AssignTable, ReadsStats) = {
    logger.info("BBH assignment")
    var t1 =  System.currentTimeMillis()
    val bestScores = mutable.HashMap[String, Int]()
    val assignment = mutable.HashMap[String, Assignment]()
    val readsStatsBuilder = new ReadsStatsBuilder()

    for (hit <- hits) {
      val taxId = getTaxIdFromRefId(hit.refId)
      taxId match {
        case None => readsStatsBuilder.addWrongRefId(hit.refId)
        case Some(tid) => {
          if (hit.score >= bestScores.getOrElse(tid, 0)) {
            bestScores.put(hit.readId, hit.score)
            assignment.put(hit.readId, Assignment(tid, hit.score))
          }
        }
      }
    }
    var t2 =  System.currentTimeMillis()
    logger.info("BBH assignment finished " + (t2 - t1) + " ms")


    logger.info("preparing results")
    t1 =  System.currentTimeMillis()
    val res = prepareAssignedResults(chunk, BBH, reads, bestScores, assignment, readsStatsBuilder.build)
    t2 =  System.currentTimeMillis()
    logger.info("preparing results finished " + (t2 - t1) + " ms")

    res
  }

}
