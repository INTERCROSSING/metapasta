package ohnosequences.metapasta.reporting

import ohnosequences.metapasta._
import scala.collection.mutable
import ohnosequences.nisperon.{AWS, intMonoid}
import ohnosequences.metapasta.reporting.spreadsheeet._
import ohnosequences.awstools.s3.ObjectAddress

object FileType {
  type Item = (TaxonId, (TaxonInfo, mutable.HashMap[(SampleId, AssignmentType), PerSampleData]))
}

trait FileType {
  def attributes(samples: List[SampleId]): List[AnyAttribute.For[FileType.Item]]

  def destination(dst: ObjectAddress): ObjectAddress
}

object FileTypeA extends FileType {


  import FileType.Item

  override def destination(dst: ObjectAddress): ObjectAddress = {
    dst / "project.A.frequencies.csv"
  }

  val unassigned = TaxonId("unassigned")

  val emptyStringMonoid = new StringConstantMonoid("")

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

  def attributes(samples: List[SampleId]) = {

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

}

object FileTypeB extends FileType {

  override def destination(dst: ObjectAddress): ObjectAddress = {
    dst / "project.B.frequencies.csv"
  }

  import FileType.Item

  val unassigned = TaxonId("unassigned")

  val emptyStringMonoid = new StringConstantMonoid("")


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

  def attributes(samples: List[SampleId]) = {
    val res = new mutable.ListBuffer[AnyAttribute.For[Item]]()
    res += taxonomyName

    for (sample <- samples) {
      for (assignmentType <- List(BBH, LCA)) {
        val sd = SampleDirect(sample, assignmentType)
        res += sd      }
    }
    res.toList
  }

}

object FileTypeC extends FileType {
  import FileType.Item

  override def destination(dst: ObjectAddress): ObjectAddress = {
    dst / "project.C.frequencies.csv"
  }

  val unassigned = TaxonId("unassigned")

  val emptyStringMonoid = new StringConstantMonoid("")


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

  def attributes(samples: List[SampleId]) = {
    val res = new mutable.ListBuffer[AnyAttribute.For[Item]]()
    res += taxonomyName

    for (sample <- samples) {
      for (assignmentType <- List(BBH, LCA)) {
        val sd = SampleDirect(sample, assignmentType)
        res += sd
        res += Freq(sd)
      }
    }
    res.toList
  }

}

