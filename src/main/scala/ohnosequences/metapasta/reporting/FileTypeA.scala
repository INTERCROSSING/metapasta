package ohnosequences.metapasta.reporting

import ohnosequences.metapasta._
import scala.collection.mutable
import ohnosequences.nisperon.{AWS, doubleMonoid, intMonoid}
import ohnosequences.metapasta.AssignTable
import ohnosequences.awstools.s3.ObjectAddress


//generate table for all samples

//item: TaxonomyID TaxonomyName TaxonomyRank Sample(direct.absolute.freq, direct.relative.freq, cumulative.absolute.freq, cumulative.relative.freq)

class PerSampleData(var direct: Int, var directFreq: Double = 0, var cumulative: Int, var cumulativeFreq: Double = 0)

case class TaxonId(id: String)

case class TaxonInfo(scientificName: String, rank: String)

case class SampleId(id: String)

class FileTypeA(aws: AWS, destination: ObjectAddress, nodeRetriever: NodeRetriever, assignments: Map[String, AssignTable], samples: List[SampleId]) {
  def getTaxInfo(taxId: TaxonId) = {
    try {
       val node = nodeRetriever.nodeRetriever.getNCBITaxonByTaxId(taxId.id)
       TaxonInfo(node.getScientificName(), node.getRank())
    } catch {
       case t: Throwable => TaxonInfo("na", "na")
    }
  }

  val emptyStringMonoid = new StringConstantMonoid("")

  object taxId extends StringAttribute("TaxonomyID", new StringConstantMonoid("total"))

  object taxonomyName extends StringAttribute("TaxonomyName", emptyStringMonoid)
  object taxonomyRank extends StringAttribute("TaxonomyRank", emptyStringMonoid)

  case class SampleDirect(sampleId: SampleId, assignmentType: AssignmentType) extends IntAttribute(sampleId.id + "." + assignmentType + ".direct.absolute", intMonoid)
  case class SampleDirectFreq(sampleId: SampleId, assignmentType: AssignmentType) extends DoubleAttribute(sampleId.id + "." + assignmentType + ".direct.freq", doubleMonoid)
  case class SampleCumulative(sampleId: SampleId, assignmentType: AssignmentType) extends IntAttribute(sampleId.id + "." + assignmentType + ".cumulative.absolute", intMonoid)
  case class SampleCumulativeFreq(sampleId: SampleId, assignmentType: AssignmentType) extends DoubleAttribute(sampleId.id + "." + assignmentType + ".cumulative.freq", doubleMonoid)

  def attributes() = {

    val res = new mutable.ListBuffer[AnyAttributeType]()

    res += taxId
    res += taxonomyName
    res += taxonomyRank

    for (sample <- samples) {
      for (attributeType <- List(BBH, LCA)) {
        res += SampleDirect(sample, attributeType)
        res += SampleDirectFreq(sample, attributeType)
        res += SampleCumulative(sample, attributeType)
        res += SampleCumulativeFreq(sample, attributeType)
      }
    }
    res.toList
  }

  def f(item: (TaxonId, (TaxonInfo, mutable.HashMap[AssignmentType, mutable.HashMap[SampleId, PerSampleData]])), a: AnyAttributeType): AnyCellValue = {
    val map = item._2._2
    a match {
      case t: taxId.type => t.wrap(item._1.id)
      case t: taxonomyName.type => t.wrap(item._2._1.scientificName)
      case t: taxonomyRank.type => t.wrap(item._2._1.rank)

      case s @ SampleDirect(sample, assignmentType) => map.get(assignmentType).flatMap(_.get(sample)).map { sampleInfo => s.wrap(sampleInfo.direct) }.getOrElse(s.default)
      case s @ SampleDirectFreq(sample, assignmentType) => map.get(assignmentType).flatMap(_.get(sample)).map { sampleInfo => s.wrap(sampleInfo.directFreq) }.getOrElse(s.default)
      case s @ SampleCumulative(sample, assignmentType) => map.get(assignmentType).flatMap(_.get(sample)).map { sampleInfo => s.wrap(sampleInfo.cumulative) }.getOrElse(s.default)
      case s @ SampleCumulativeFreq(sample, assignmentType) => map.get(assignmentType).flatMap(_.get(sample)).map { sampleInfo => s.wrap(sampleInfo.cumulativeFreq) }.getOrElse(s.default)

    }
  }

  def generateCSV() = {
    val prepareItems = prepareMapping()
    val csvPrinter = new CSVPrinter(aws, prepareItems, attributes(), f)
    csvPrinter.generate(destination)


  }

  def prepareMapping(): mutable.HashMap[TaxonId, (TaxonInfo, mutable.HashMap[AssignmentType, mutable.HashMap[SampleId, PerSampleData]])] = {
    val result = new mutable.HashMap[TaxonId, (TaxonInfo, mutable.HashMap[AssignmentType, mutable.HashMap[SampleId, PerSampleData]])]()

    val directTotal = new mutable.HashMap[AssignmentType, mutable.HashMap[SampleId, Int]]()
    val cumulativeTotal = new mutable.HashMap[AssignmentType, mutable.HashMap[SampleId, Int]]()

    for ((assignmentTypeRaw, assignmentTable) <- assignments) {
      val assignmentType = AssignmentType.fromString(assignmentTypeRaw)
      for ((sample, taxonsInfos) <-  assignmentTable.table) {
        for ((taxon, taxonInfo) <- taxonsInfos) {

          val sampleId = SampleId(sample)
          val taxonId = TaxonId(taxon)

          directTotal.get(assignmentType) match {
            case None => {
              val map = new mutable.HashMap[SampleId, Int]()
              map.put(sampleId, taxonInfo.count)
              directTotal.put(assignmentType, map)
            }
            case Some(map) => {
              map.put(sampleId, map.getOrElse(sampleId, 0) + taxonInfo.count)
              directTotal.put(assignmentType, map)
            }
          }

          cumulativeTotal.get(assignmentType) match {
            case None => {
              val map = new mutable.HashMap[SampleId, Int]()
              map.put(sampleId, taxonInfo.acc)
              cumulativeTotal.put(assignmentType, map)
            }
            case Some(map) => {
              map.put(sampleId, map.getOrElse(sampleId, 0) + taxonInfo.acc)
              cumulativeTotal.put(assignmentType, map)
            }
          }

          result.get(taxonId) match {
            case None => {
              val map0 = new mutable.HashMap[AssignmentType, mutable.HashMap[SampleId, PerSampleData]]()
              val map1 = new mutable.HashMap[SampleId, PerSampleData]()
              map1.put(sampleId, new PerSampleData(direct = taxonInfo.count, cumulative = taxonInfo.acc))
              map0.put(assignmentType, map1)
              result.put(taxonId, (getTaxInfo(taxonId), map0))
            }
            case Some((taxoninfo, map0)) => {
              map0.get(assignmentType) match {
                case None => {
                  val map1 = new mutable.HashMap[SampleId, PerSampleData]()
                  map1.put(sampleId, new PerSampleData(direct = taxonInfo.count, cumulative = taxonInfo.acc))
                  map0.put(assignmentType, map1)
                }
                case Some(map1) => {
                  map1.put(sampleId, new PerSampleData(direct = taxonInfo.count, cumulative = taxonInfo.acc))
                }
              }
            }
          }
        }
      }
    }

    for ((taxonId, (taxonInfo, map0)) <- result) {
      for ((assignmentType, map1) <- map0) {
        for ((sampleId, persampledata) <- map1) {
          persampledata.directFreq = persampledata.direct / directTotal(assignmentType)(sampleId)
          persampledata.cumulativeFreq = persampledata.cumulative / cumulativeTotal(assignmentType)(sampleId)
        }
      }
    }
    result
  }
}
