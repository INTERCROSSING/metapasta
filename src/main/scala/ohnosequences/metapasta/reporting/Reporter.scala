package ohnosequences.metapasta.reporting

import ohnosequences.metapasta._
import org.clapper.avsl.Logger
import ohnosequences.nisperon._
import ohnosequences.awstools.s3.ObjectAddress
import ohnosequences.metapasta.ReadsStats
import ohnosequences.metapasta.AssignTable
import scala.collection.mutable
import ohnosequences.metapasta.reporting.spreadsheeet.CSVExecutor
import scala.collection.mutable.ListBuffer


case class PerSampleData(direct: Int, cumulative: Int)

case class TaxonId(id: String)

case class TaxonInfo(scientificName: String, rank: String)

case class SampleId(id: String)

case class SampleTag(tag: String)

class Reporter(aws: AWS,
               tablesAddresses: List[ObjectAddress],
               statsAddresses: List[ObjectAddress],
               tags: Map[SampleId, List[SampleTag]],
               nodeRetriever: NodeRetriever,
               destination: ObjectAddress) {

  val assignmentTableSerializer = ohnosequences.metapasta.assignTableSerializer
  val statsMonoid = new MapMonoid[(String, AssignmentType), ReadsStats](readsStatsMonoid)
  val statsSerializer = readsStatsSerializer

  def read[T](address: ObjectAddress, serializer: Serializer[T]): T = {
     serializer.fromString(aws.s3.readWholeObject(address))
  }

  val logger = Logger(this.getClass)

  def generate() {
    logger.info("reading assignment tables")

    var table: AssignTable = assignTableMonoid.unit
    for (obj <- tablesAddresses) {
      logger.info("reading from " + obj)
      val t = read[AssignTable](obj, assignmentTableSerializer)
      table = assignTableMonoid.mult(table, t)
    }

    var stats: Map[(String, AssignmentType), ReadsStats] = statsMonoid.unit

    logger.info("reading reads stats")
    for (obj <- statsAddresses) {
      logger.info("reading from " + obj)
      val t = read[Map[(String, AssignmentType), ReadsStats]](obj, statsSerializer)
      stats = statsMonoid.mult(stats, t)
    }

    val samplesS: Set[String] = stats.keySet.map(_._1)
    val samples = samplesS.toList.map(SampleId)
    val pg = ProjectGroup(samples)

    val groups: List[AnyGroup] = generateGroups(samples, tags) :+ pg

    for (group <- groups) {
      val groupStats = stats.filterKeys {
        sampleAssignmentType =>
          group.samples.map(_.id).contains(sampleAssignmentType._1)
      }

      val groupTable = AssignTable(table.table.filterKeys {
        sampleAssignmentType =>
          group.samples.map(_.id).contains(sampleAssignmentType._1)
      })
      projectSpecific(groupTable, groupStats, group)
    }
  }

  def generateGroups(samples: List[SampleId], tags: Map[SampleId, List[SampleTag]]): List[SamplesGroup] = {
    logger.info("generating groups")
    val allTags = new mutable.HashSet[SampleTag]()
    for ((sample, sampleTags) <- tags) {
      allTags ++= sampleTags
    }

    val groups = new ListBuffer[SamplesGroup]()

    for (tag <- allTags) {
      groups += SamplesGroup(tag.tag + "+", tags.filter(_._2.contains(tag)).keys.toList)
      groups += SamplesGroup(tag.tag + "-", tags.filterNot(_._2.contains(tag)).keys.toList)
    }
    groups.toList
  }



  //stats should contain only stats for group.samples
  def projectSpecific(table: AssignTable, stats: Map[(String, AssignmentType), ReadsStats], group: AnyGroup) {

    group match {
      case ProjectGroup(s) => logger.info("generating project specific files")
      case SamplesGroup(name, s) => logger.info("generating grouo specific files for group: " + name)
    }


    val ranks: List[Option[TaxonomyRank]] = TaxonomyRank.ranks.map(Some(_)) ++ List(None)

    val unassigned = new mutable.HashMap[(SampleId, AssignmentType), PerSampleData]()
    val unassignedItem = (FileType.unassigned, (TaxonInfo("", ""),unassigned))

    for ((sampleAssignmentType, stat) <- stats) {
      val sampleId = SampleId(sampleAssignmentType._1)
      val assignmentType = sampleAssignmentType._2
      val d = (stat.total - stat.assigned).toInt
      unassigned.put((sampleId, assignmentType), PerSampleData(d, d))
    }


    val fileTypes = group match {
      case pg @ ProjectGroup(s) => List(FileTypeA(pg), FileTypeB(pg), FileTypeC(pg))
      case g @ SamplesGroup(name, s) => List(FileTypeA(g), FileTypeD(g))
    }

    for (r <- ranks) {

      r match {
        case None => logger.info("generating for all kinds")
        case Some(rr) => logger.info("generating for " + rr + " kind")
      }

      val items: Iterable[FileType.Item] =  prepareMapping(table, r) += unassignedItem

      for (fType <- fileTypes) {
        val csvPrinter = new CSVExecutor[FileType.Item](fType.attributes(), items)
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
      val assignmentType = sampleAssignmentType._2

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
