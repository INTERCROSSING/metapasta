package ohnosequences.metapasta.reporting

import ohnosequences.metapasta._
import scala.collection.mutable
import ohnosequences.nisperon.{AWS, intMonoid}
import ohnosequences.metapasta.reporting.spreadsheeet._
import ohnosequences.awstools.s3.ObjectAddress

object FileType {
  type Item = (TaxonId, (TaxonInfo, mutable.HashMap[(SampleId, AssignmentType), PerSampleData]))

  val unassigned = TaxonId("unassigned")
  val emptyStringMonoid = new StringConstantMonoid("")
}

trait FileType {
  def attributes(): List[AnyAttribute.For[FileType.Item]]

  def destination(dst: ObjectAddress): ObjectAddress

}

case class FileTypeA(group: AnyGroup, rank: Option[TaxonomyRank]) extends FileType {

  import FileType.{Item, emptyStringMonoid}

  override def destination(dst: ObjectAddress): ObjectAddress = {
    rank match {
      case None => dst / (group.name + ".frequencies.csv")
      case Some(rr) => dst / (group.name + "." + rr.toString + ".frequencies.csv")
    }

  }

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
      item._2._2.get(sampleId -> assignmentType).map(_.direct).getOrElse(0)
    }
  }


  case class SampleCumulative(sampleId: SampleId, assignmentType: AssignmentType) extends IntAttribute[Item](sampleId.id + "." + assignmentType + ".cumulative.absolute", intMonoid) {
   override def execute(item: Item, index: Int, context: Context): Int = {
     item._2._2.get(sampleId -> assignmentType).map(_.cumulative).getOrElse(0)
   }
 }

  def attributes() = {

    val res = new mutable.ListBuffer[AnyAttribute.For[Item]]()

    res += taxId
    res += taxonomyName
    res += taxonomyRank

    for (sample <- group.samples) {
      for (assignmentType <- List(BBH, LCA)) {
        val sd = SampleDirect(sample, assignmentType)
        res += sd
        res += Freq(sd)

        val sc = SampleCumulative(sample, assignmentType)
        res += sc

        res += Normalize(sc, sd)
      }
    }
    res.toList
  }

}

case class FileTypeB(project: ProjectGroup) extends FileType {

  override def destination(dst: ObjectAddress): ObjectAddress = {
    dst / "direct.absolute.freq.csv"
  }

  import FileType.{Item}

  object taxonomyName extends StringAttribute[Item]("TaxonomyName",  new StringConstantMonoid("total")) {
    override def execute(item: Item, index: Int, context: Context): String = {
      item._2._1.scientificName
    }
  }

  case class SampleDirect(sampleId: SampleId, assignmentType: AssignmentType) extends IntAttribute[Item](sampleId.id + "." + assignmentType + ".direct.absolute", intMonoid) {
    override def execute(item: Item, index: Int, context: Context): Int = {
      item._2._2.get(sampleId -> assignmentType).map(_.direct).getOrElse(0)
    }
  }

  def attributes() = {


    val sampleAttributes = new mutable.ListBuffer[SampleDirect]()
    for (sample <- project.samples) {
      for (assignmentType <- List(BBH, LCA)) {
        val sd = SampleDirect(sample, assignmentType)
        sampleAttributes += sd      }
    }

    val res = new mutable.ListBuffer[AnyAttribute.For[Item]]()
    res += taxonomyName
    res ++= sampleAttributes
    for ((assignmentType, attrs) <- sampleAttributes.groupBy(_.assignmentType)) {
      res += Sum(attrs.toList)
    }

    res.toList
  }

}

case class FileTypeC(project: ProjectGroup) extends FileType {

  import FileType.Item

  override def destination(dst: ObjectAddress): ObjectAddress = {
    dst / "direct.relative.freq.csv"
  }

  object taxonomyName extends StringAttribute[Item]("TaxonomyName",  new StringConstantMonoid("total")) {
    override def execute(item: Item, index: Int, context: Context): String = {
      item._2._1.scientificName
    }
  }

  case class SampleDirect(sampleId: SampleId, assignmentType: AssignmentType) extends IntAttribute[Item](sampleId.id + "." + assignmentType + ".direct.absolute", intMonoid, hidden = true) {
    override def execute(item: Item, index: Int, context: Context): Int = {
      item._2._2.get(sampleId -> assignmentType).map(_.direct).getOrElse(0)
    }
  }

  def attributes() = {
    val res = new mutable.ListBuffer[AnyAttribute.For[Item]]()
    res += taxonomyName

    for (sample <- project.samples) {
      for (assignmentType <- List(BBH, LCA)) {
        val sd = SampleDirect(sample, assignmentType)
        res += sd
        res += Freq(sd)
      }
    }
    res.toList
  }

}

case class FileTypeD(group: SamplesGroup) extends FileType {

  import FileType.{Item, emptyStringMonoid}

  override def destination(dst: ObjectAddress): ObjectAddress = {
    dst / ("frequencies.complete.csv")
  }


  object taxonomyName extends StringAttribute[Item]("TaxonomyName", emptyStringMonoid) {
    override def execute(item: Item, index: Int, context: Context): String = {
      item._2._1.scientificName
    }
  }

  case class SampleDirect(sampleId: SampleId, assignmentType: AssignmentType) extends IntAttribute[Item](sampleId.id + "." + assignmentType + ".direct.absolute", intMonoid, true) {
    override def execute(item: Item, index: Int, context: Context): Int = {
      item._2._2.get(sampleId -> assignmentType).map(_.direct).getOrElse(0)
    }
  }


  case class SampleCumulative(sampleId: SampleId, assignmentType: AssignmentType) extends IntAttribute[Item](sampleId.id + "." + assignmentType + ".cumulative.absolute", intMonoid, true) {
    override def execute(item: Item, index: Int, context: Context): Int = {
      item._2._2.get(sampleId -> assignmentType).map(_.cumulative).getOrElse(0)
    }
  }

  def attributes() = {

    val res = new mutable.ListBuffer[AnyAttribute.For[Item]]()

    res += taxonomyName

    val relDirect = new mutable.ListBuffer[(AssignmentType, DoubleAttribute[Item])]()
    val relCumulative = new mutable.ListBuffer[(AssignmentType, DoubleAttribute[Item])]()

    for (sample <- group.samples) {
      for (assignmentType <- List(BBH, LCA)) {
        val sd = SampleDirect(sample, assignmentType)
        res += sd
        val rd = Freq(sd)
        res += rd
        relDirect += ((assignmentType, rd))

        val sc = SampleCumulative(sample, assignmentType)
        res += sc
        val rc = Normalize(sc, sd)
        res += rc
        relCumulative += ((assignmentType, rc))
      }
    }

    for ((assignmentType, attrs) <- relDirect.groupBy(_._1)) {
      res += Average(attrs.toList.map(_._2))
    }

    res.toList
  }

}

