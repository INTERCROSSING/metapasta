package ohnosequences.metapasta

import ohnosequences.nisperon.{JsonSerializer, Serializer, MapMonoid, Monoid}
import scala.collection.mutable


//todo rank, name ...
case class TaxInfo(count: Long, acc: Long) {
  override def toString = count + ":" + acc
}

object taxInfoMonoid extends Monoid[TaxInfo] {
  val _unit = TaxInfo(0, 0)
  def unit: TaxInfo = _unit
  def mult(x: TaxInfo, y: TaxInfo): TaxInfo = TaxInfo(x.count + y.count, x.acc + y.acc)
}



//(sample, AssignmentType -> (tax -> taxinfo)

case class AssignTable(table: Map[(String, AssignmentType), Map[String, TaxInfo]])


object assignTableMonoid extends Monoid[AssignTable] {
  val mapMonoid = new MapMonoid[(String, AssignmentType), Map[String, TaxInfo]](new MapMonoid[String, TaxInfo](taxInfoMonoid))

  override def mult(x: AssignTable, y: AssignTable): AssignTable = AssignTable(mapMonoid.mult(x.table, y.table))

  override def unit: AssignTable = AssignTable(mapMonoid.unit)
}


object assignTableSerializer extends Serializer[AssignTable] {

  val rawTableSerializer = new JsonSerializer[Map[String,  Map[String, TaxInfo]]]()

  override def toString(t: AssignTable): String = {
    val raw: Map[String,  Map[String, TaxInfo]] = t.table.map { case (sampleAssignmentType, map)  =>
      (sampleAssignmentType._1 + "###" + sampleAssignmentType._2.toString, map)
    }
    rawTableSerializer.toString(raw)
  }

  override def fromString(s: String): AssignTable = {
    val raw : Map[String,  Map[String, TaxInfo]] = rawTableSerializer.fromString(s)
    AssignTable(raw.map { case (sampleAssignmentType, stats)  =>
      val parts = sampleAssignmentType.split("###")
      ((parts(0), AssignmentType.fromString(parts(1))), stats)
    })
  }
}