package ohnosequences.metapasta.reporting

import ohnosequences.metapasta._
import scala.collection.mutable
import ohnosequences.nisperon.{AWS, doubleMonoid, intMonoid}
import ohnosequences.metapasta.AssignTable
import ohnosequences.awstools.s3.ObjectAddress
import ohnosequences.metapasta.reporting.spreadsheeet._
import ohnosequences.metapasta.reporting.SampleId
import ohnosequences.metapasta.reporting.TaxonInfo
import ohnosequences.metapasta.reporting.spreadsheeet.StringConstantMonoid
import scala.Some
import ohnosequences.awstools.s3.ObjectAddress
import ohnosequences.metapasta.reporting.TaxonId
import ohnosequences.metapasta.AssignTable


//generate table for all samples

//item: TaxonomyID TaxonomyName TaxonomyRank Sample(direct.absolute.freq, direct.relative.freq, cumulative.absolute.freq, cumulative.relative.freq)

class PerSampleData(val direct: Int, val cumulative: Int)

case class TaxonId(id: String)

case class TaxonInfo(scientificName: String, rank: String)

case class SampleId(id: String)

class FileTypeA(aws: AWS, destination: ObjectAddress, nodeRetriever: NodeRetriever, assignments: Map[String, AssignTable], samples: List[SampleId]) {
  def getTaxInfo(taxId: TaxonId) = {
    try {
       val node = nodeRetriever.nodeRetriever.getNCBITaxonByTaxId(taxId.id)
       TaxonInfo(node.getScientificName(), node.getRank())
    } catch {
       case t: Throwable => t.printStackTrace(); TaxonInfo("na", "na")
    }
  }

  val emptyStringMonoid = new StringConstantMonoid("")

  type Item = (TaxonId, (TaxonInfo, mutable.HashMap[AssignmentType, mutable.HashMap[SampleId, PerSampleData]]))

  object taxId extends StringAttribute[Item]("TaxonomyID", new StringConstantMonoid("total")) {
    override def execute(item: Item, index: Int, context: Context): String = {
      item._1.id
    }
  }

  object taxonomyName extends StringAttribute[Item]("TaxonomyName", emptyStringMonoid) {
    override def execute(item: Item, index: Int, context: Context): String = {
      item._2._1.scientificName
    }
  }

  object taxonomyRank extends StringAttribute[Item]("TaxonomyRank", emptyStringMonoid) {
    override def execute(item: Item, index: Int, context: Context): String = {
      item._2._1.rank
    }
  }

  case class SampleDirect(sampleId: SampleId, assignmentType: AssignmentType) extends IntAttribute[Item](sampleId.id + "." + assignmentType + ".direct.absolute", intMonoid) {
    override def execute(item: Item, index: Int, context: Context): Int = {
      item._2._2.get(assignmentType).flatMap(_.get(sampleId)).map(_.direct).getOrElse(0)
    }
  }


 // case class SampleDirectFreq(sampleId: SampleId, assignmentType: AssignmentType) extends DoubleAttribute(sampleId.id + "." + assignmentType + ".direct.freq", doubleMonoid)
  case class SampleCumulative(sampleId: SampleId, assignmentType: AssignmentType) extends IntAttribute[Item](sampleId.id + "." + assignmentType + ".cumulative.absolute", intMonoid) {
   override def execute(item: Item, index: Int, context: Context): Int = {
     item._2._2.get(assignmentType).flatMap(_.get(sampleId)).map(_.cumulative).getOrElse(0)
   }
 }
  //case class SampleCumulativeFreq(sampleId: SampleId, assignmentType: AssignmentType) extends DoubleAttribute(sampleId.id + "." + assignmentType + ".cumulative.freq", doubleMonoid)

  def attributes() = {

    val res = new mutable.ListBuffer[AnyAttribute.For[Item]]()

    res += taxId
    res += taxonomyName
    res += taxonomyRank

    for (sample <- samples) {
      for (assignmentType <- List(BBH, LCA)) {
        val sd = SampleDirect(sample, assignmentType)
        res += sd
        res += Freq(sd)

        val sc = SampleCumulative(sample, assignmentType)
        res += sc
        res += Freq(sc)
      }
    }
    res.toList
  }


  def generateCSV() = {
    val prepareItems = prepareMapping()
    val csvPrinter = new CSVExecutor[Item](attributes(), prepareItems)
    val res = csvPrinter.execute()
    aws.s3.putWholeObject(destination, res)


  }

  def prepareMapping(): mutable.HashMap[TaxonId, (TaxonInfo, mutable.HashMap[AssignmentType, mutable.HashMap[SampleId, PerSampleData]])] = {
    val result = new mutable.HashMap[TaxonId, (TaxonInfo, mutable.HashMap[AssignmentType, mutable.HashMap[SampleId, PerSampleData]])]()


    for ((assignmentTypeRaw, assignmentTable) <- assignments) {
      val assignmentType = AssignmentType.fromString(assignmentTypeRaw)
      for ((sample, taxonsInfos) <-  assignmentTable.table) {
        for ((taxon, taxonInfo) <- taxonsInfos) {

          val sampleId = SampleId(sample)
          val taxonId = TaxonId(taxon)

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

    result
  }
}
