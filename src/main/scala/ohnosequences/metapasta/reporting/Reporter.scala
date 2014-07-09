package ohnosequences.metapasta.reporting

import ohnosequences.metapasta._
import org.clapper.avsl.Logger
import ohnosequences.nisperon._
import ohnosequences.metapasta.reporting.{PerSampleData, SampleId}
import ohnosequences.awstools.s3.ObjectAddress
import ohnosequences.metapasta.ReadsStats
import ohnosequences.metapasta.AssignTable
import scala.collection.mutable
import ohnosequences.metapasta.reporting.spreadsheeet.CSVExecutor




case class PerSampleData(direct: Int, cumulative: Int)

case class TaxonId(id: String)

case class TaxonInfo(scientificName: String, rank: String)

case class SampleId(id: String)

class Reporter(aws: AWS, tablesAddresses: List[ObjectAddress], statsAddresses: List[ObjectAddress], samples: List[SampleId], nodeRetriever: NodeRetriever, destination: ObjectAddress) {

  val assignmentTableSerializer = new JsonSerializer[AssignTable]
  val statsMonoid = new MapMonoid[(String, String), ReadsStats](readsStatsMonoid)
  val statsSerializer = new JsonSerializer[Map[(String, String), ReadsStats]]

  def read[T](address: ObjectAddress, serializer: Serializer[T]): T = {
     serializer.fromString(aws.s3.readWholeObject(address))
  }

  val logger = Logger(this.getClass)

  def read() {
    logger.info("reading assignment tables")

    var table: AssignTable = assignTableMonoid.unit
    for (obj <- tablesAddresses) {
      logger.info("reading from " + obj)
      val t = read[AssignTable](obj, assignmentTableSerializer)
      table = assignTableMonoid.mult(table, t)
    }


    var stats: Map[(String, String), ReadsStats] = statsMonoid.unit

    logger.info("reading reads stats")
    for (obj <- statsAddresses) {
      logger.info("reading from " + obj)
      val t = read[Map[(String, String), ReadsStats]](obj, statsSerializer)
      stats = statsMonoid.mult(stats, t)
    }

    val samplesS: Set[String] = stats.keySet.map(_._1)
    val samples = samplesS.toList

    projectSpecific(table, stats, samples)


  }

  def projectSpecific(table: AssignTable, stats: Map[(String, String), ReadsStats], samples: List[String]) {
    logger.info("generating project specific files")

    val ranks: List[Option[TaxonomyRank]] = TaxonomyRank.ranks.map(Some(_)) ++ List(None)

    val unassigned = new mutable.HashMap[(SampleId, AssignmentType), PerSampleData]()
    val unassignedItem = (FileTypeA.unassigned, (TaxonInfo("", ""),unassigned))

    for ((sampleAssignmentType, stat) <- stats) {
      val sampleId = SampleId(sampleAssignmentType._1)
      val assignmentType = AssignmentType.fromString(sampleAssignmentType._2)
      val d = (stat.total - stat.assigned).toInt
      unassigned.put((sampleId, assignmentType), PerSampleData(d, d))
    }

    val fileTypes  = List(FileTypeA, FileTypeB, FileTypeC)

    for (r <- ranks) {

      r match {
        case None => logger.info("generating project specific file for all kinds")
        case Some(rr) => logger.info("generating project specific file for " + rr + " kind")
      }

      val items: Iterable[FileType.Item] =  prepareMapping(table, r) += unassignedItem

      for (fType <- fileTypes) {
        val csvPrinter = new CSVExecutor[FileType.Item](fType.attributes(samples.map(SampleId(_))), items)
        val resultS = csvPrinter.execute()
        aws.s3.putWholeObject(fType.destination(destination), resultS)
      }

    }
  }

  def getTaxInfo(taxId: TaxonId) = {
    try {
      val node = nodeRetriever.nodeRetriever.getNCBITaxonByTaxId(taxId.id)
      TaxonInfo(node.getScientificName(), node.getRank())
    } catch {
      case t: Throwable => t.printStackTrace(); TaxonInfo("na", "na")
    }
  }



  def prepareMapping(table: AssignTable, rank: Option[TaxonomyRank]): mutable.HashMap[TaxonId, (TaxonInfo, mutable.HashMap[(SampleId, AssignmentType), PerSampleData])] = {

    val result = new mutable.HashMap[TaxonId, (TaxonInfo, mutable.HashMap[(SampleId, AssignmentType), PerSampleData])]()

    for ((sampleAssignmentType, taxonsInfos) <-  table.table) {

      val sampleId = SampleId(sampleAssignmentType._1)
      val assignmentType = AssignmentType.fromString(sampleAssignmentType._2)

      for ((taxon, taxonInfo) <- taxonsInfos) {

        val taxonId = TaxonId(taxon)
        val taxonInfoInfo = getTaxInfo(taxonId)

        val filter = rank match {
          case None => false
          case Some(rk) if rk.toString.equals(taxonInfoInfo.rank) => false
          case _ => true
        }

        if(!filter) {
          result.get(taxonId) match {
            case None => {
              val map = new mutable.HashMap[(SampleId, AssignmentType), PerSampleData]()
              map.put((sampleId, assignmentType), new PerSampleData(direct = taxonInfo.count, cumulative = taxonInfo.acc))
              result.put(taxonId, (taxonInfoInfo, map))
            }
            case Some((taxoninfo, map)) => {
              map.put((sampleId, assignmentType), new PerSampleData(direct = taxonInfo.count, cumulative = taxonInfo.acc))
            }
          }
        }

      }
    }

    result
  }


}
