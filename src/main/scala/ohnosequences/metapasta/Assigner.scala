package ohnosequences.metapasta

import org.clapper.avsl.Logger
import scala.collection.mutable
import ohnosequences.metapasta.databases.{GIMapper, Database16S}
import ohnosequences.formats.{RawHeader, FASTQ}
import ohnosequences.nisperon.{AWS, MapMonoid}
import ohnosequences.awstools.s3.ObjectAddress
import ohnosequences.nisperon.logging.S3Logger

case class Hit(readId: String, refId: String, score: Int)

sealed trait Assignment {
  type AssignmentCat <: AssignmentCategory
}

case class TaxIdAssignment(taxId: String, refIds: List[String]) extends Assignment {
  type AssignmentCat = Assigned.type
}

case class NoTaxIdAssignment(refIds: List[String]) extends Assignment {
  type AssignmentCat = NoTaxId.type
}

case class NotAssigned(reason: String, refIds: List[String], taxIds: List[String]) extends Assignment {
  type AssignmentCat = NotAssignedCat.type
}



class Assigner(aws: AWS,
               nodeRetriever: NodeRetriever,
               database: Database16S,
               giMapper: GIMapper,
               assignmentConfiguration: AssignmentConfiguration,
               extractReadHeader: String => String,
               logging: Boolean,
               readsDirectory: ObjectAddress) {


  val logger = Logger(this.getClass)

  def assign(chunk: MergedSampleChunk, reads: List[FASTQ[RawHeader]], hits: List[Hit],
             s3logger: S3Logger): (AssignTable, Map[(String, AssignmentType), ReadsStats]) = {

    val lcaRes = assignLCA(chunk, reads, hits, assignmentConfiguration.bitscoreThreshold, assignmentConfiguration.p, s3logger)
    val bbhRes = assignBestHit(chunk, reads, hits, s3logger)


    (assignTableMonoid.mult(lcaRes._1, bbhRes._1), Map((chunk.sample, LCA) -> lcaRes._2, (chunk.sample, BBH) -> bbhRes._2))


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
    if (node == null) {
      logger.error("can't receive node for " + tax)
    } else {
      var parent = node.getParent()
      while (parent != null) {
        res += parent.getTaxId()
        parent = parent.getParent()
      }
    }
    res.toList
  }


  def prepareAssignedResults(s3logger: S3Logger, chunk: MergedSampleChunk,
                             assignmentType: AssignmentType,
                             reads: List[FASTQ[RawHeader]],
                             assignment: mutable.HashMap[String, Assignment],
                             initialReadsStats: ReadsStats = readsStatsMonoid.unit): (AssignTable, ReadsStats) = {

    val readsStatsBuilder = new ReadStatsBuilder()
    val fastasWriter = new FastasWriter(s3logger, nodeRetriever, logging)

    reads.foreach {
      fastq =>
        val readId = extractReadHeader(fastq.header.getRaw)
        assignment.get(readId) match {
          case None => {
            //nohit
            fastasWriter.writeNoHit(fastq, readId)
            s3logger.info("no hits for read id " + readId)
            readsStatsBuilder.incrementByCategory(NoHit)
          }
          case Some(assignment) => {
            fastasWriter.write(chunk, fastq, readId, assignment)
            s3logger.info(assignment + " for " + readId)
            readsStatsBuilder.incrementByAssignment(assignment)
          }
        }
    }

    fastasWriter.uploadFastas(aws, readsDirectory, chunk, assignmentType)

    val assignTable = mutable.HashMap[String, TaxInfo]()

    //generate assign table
    assignment.foreach {
      case (readId, TaxIdAssignment(taxId, refIds)) =>
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
      case _ => ()
    }

    val readStats = readsStatsBuilder.build

    assignTable.put(NoHit.taxId, TaxInfo(readStats.noHit, readStats.noHit))
    assignTable.put(NoTaxId.taxId, TaxInfo(readStats.noTaxId, readStats.noTaxId))
    assignTable.put(NotAssignedCat.taxId, TaxInfo(readStats.notAssigned, readStats.notAssigned))

    (AssignTable(Map((chunk.sample, assignmentType) -> assignTable.toMap)), readStats.mult(initialReadsStats))
  }

  def assignLCA(chunk: MergedSampleChunk, reads: List[FASTQ[RawHeader]], hits: List[Hit], scoreThreshold: Int, p: Double,
                s3logger: S3Logger): (AssignTable, ReadsStats) = {

    logger.info("LCA assignment")
    var t1 = System.currentTimeMillis()

    val readsStatsBuilder = new ReadStatsBuilder()

    val hitsPerReads = mutable.HashMap[String, mutable.HashSet[String]]()

    //reads to best scores
    val bestScores = mutable.HashMap[String, Int]()

    //find best scores
    for (hit <- hits) {
      if (hit.score >= bestScores.getOrElse(hit.readId, 0)) {
        bestScores.put(hit.readId, hit.score)
      }
    }

    //create set of ref ids for all hits
    hits.foreach {
      //filter all reads with bitscore below p * S (where p is fixed coefficient, e.g. 0.9)
      case hit if hit.score >= math.max(scoreThreshold, p * bestScores.getOrElse(hit.readId, 0)) => {
        hitsPerReads.get(hit.readId) match {
          case None => hitsPerReads.put(hit.readId, mutable.HashSet[String](hit.refId))
          case Some(listBuffer) => listBuffer += hit.refId
        }
      }
      case filteredHit => {
        s3logger.warn("hit " + filteredHit + " has been filtered; best score for read id is " + bestScores.getOrElse(filteredHit.readId, 0))
        hitsPerReads.put(filteredHit.readId, mutable.HashSet[String]())
      }
    }


    val finalAssignments = mutable.HashMap[String, Assignment]()

    for ((readId, refIds) <- hitsPerReads) {

      // get taxa ids from GIs
      val taxIds = new mutable.HashMap[String, String]()
      for (refId <- refIds) {
        getTaxIdFromRefId(refId) match {
          case Some(taxId) => taxIds.put(refId, taxId)
          case None => {
            s3logger.warn("couldn't find taxon for ref id: " + refId)
            readsStatsBuilder.addWrongRefId(refId)
          }
        }
      }

      //now there are four cases:
      //* we had some not filtered hits, but all of them have wrong gi - NoTaxIdAssignment
      //* we have empty ref ids that means that all hits were filtered -  NotAssigned
      // * rest hits form a line in the taxonomy tree. In this case we should choose the most specific tax id
      // * in other cases we should calculate LCA
      val assignment = if (taxIds.isEmpty && !refIds.isEmpty) {
        //couldn't get taxa from any of ref id
        NoTaxIdAssignment(refIds.toList)
      } else if (taxIds.isEmpty && refIds.isEmpty) {
        //nothing to assign
        NotAssigned("hits were filtered", refIds.toList, taxIds.values.toList)
      } else {
        isInLine(taxIds.values.toSet) match {
          case Some(specific) => {
            s3logger.info("taxa form a line: " + taxIds.values.toList)
            s3logger.info("the most specific taxon: " + specific)
            val specificRefIds: List[String] = taxIds.find {
              _._2.equals(specific)
            }.map(_._1).toList

            TaxIdAssignment(specific, specificRefIds)
          }
          case None => {
            //calculating lca
            var res: Option[String] = taxIds.headOption.map(_._2) //initial taxon
            for ((refId, taxId) <- taxIds) {
              res = res.flatMap(lca(_, taxId, s3logger))
              if(res.isEmpty) {
                s3logger.warn("can't calculate lca(" + res + ", " + taxId + ")")
              }
            }
            res match {
              case None => {
                NotAssigned("can't calculate lca for taxa", taxIds.keysIterator.toList, taxIds.values.toList)
              }
              case Some(rr) => {
                TaxIdAssignment(rr, refIds.toList)
              }
            }
          }
        }
      }
      finalAssignments.put(readId, assignment)
    }

    var t2 = System.currentTimeMillis()
    logger.info("LCA assignment finished " + (t2 - t1) + " ms")

    logger.info("preparing results")
    t1 = System.currentTimeMillis()
    //generate stats
    val res = prepareAssignedResults(s3logger, chunk, LCA, reads, finalAssignments, readsStatsBuilder.build)
    t2 = System.currentTimeMillis()
    logger.info("preparing results finished " + (t2 - t1) + " ms")

    res
  }


  //return most specific one if true
  //most specific is one that have longest parent line
  def isInLine(taxIds: Set[String]): Option[String] = {
    if (taxIds.isEmpty) {
      None
    } else if (taxIds.size == 1) {
      Some(taxIds.head)
    } else {
      var max = 0
      var argmax = List[String]()

      for (taxId <- taxIds) {
        val c = List(taxId) ++ getParentsIds(taxId)
        if (c.size > max) {
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
  def lca(tax1: String, tax2: String, s3logger: S3Logger): Option[String] = {
    if (tax1.equals(tax2)) {
      Some(tax1)
    } else {
      val par1 = List(tax1) ++ getParentsIds(tax1)
      val par2 = List(tax2) ++ getParentsIds(tax2)
      var r = "1"

      var p1 = par1.size - 1
      var p2 = par2.size - 1

      while ((p1 >= 0) && (p2 >= 0) && par1(p1).equals(par2(p2))) {
        r = par1(p1)
        p1 -= 1
        p2 -= 1
      }

      if (p1 < 0 || p2 < 0) {
       // s3logger.warn("reached root: lca(" + tax1 + ", " + tax2 + ") p_1=" + par1 + " p_2=" + par2)
      }

      s3logger.info("lca(" + tax1 + ", " + tax2 + ")=" + r + " p_1=" + p1 + " p2=" + p2)

      if (r.isEmpty) None else Some(r)
    }
  }


  def assignBestHit(chunk: MergedSampleChunk, reads: List[FASTQ[RawHeader]], hits: List[Hit],
                    s3logger: S3Logger): (AssignTable, ReadsStats) = {
    logger.info("BBH assignment")
    var t1 = System.currentTimeMillis()
    val bestScores = mutable.HashMap[String, Int]()
    val assignment = mutable.HashMap[String, Assignment]()
    val readsStatsBuilder = new ReadStatsBuilder()

    for (hit <- hits) {
      getTaxIdFromRefId(hit.refId) match {
        case None => {
          readsStatsBuilder.addWrongRefId(hit.refId)
          assignment.put(hit.readId, NoTaxIdAssignment(List(hit.refId)))
        }
        case Some(tid) => {
          if (hit.score >= bestScores.getOrElse(tid, 0)) {
            bestScores.put(hit.readId, hit.score)
            assignment.put(hit.readId, TaxIdAssignment(tid, List(hit.readId)))
          }
        }
      }
    }
    var t2 = System.currentTimeMillis()
    logger.info("BBH assignment finished " + (t2 - t1) + " ms")

    logger.info("preparing results")
    t1 = System.currentTimeMillis()
    val res = prepareAssignedResults(s3logger, chunk, BBH, reads, assignment, readsStatsBuilder.build)
    t2 = System.currentTimeMillis()
    logger.info("preparing results finished " + (t2 - t1) + " ms")

    res
  }

}
