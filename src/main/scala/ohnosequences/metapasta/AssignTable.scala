package ohnosequences.metapasta

import ohnosequences.nisperon.{MapMonoid, Monoid}
import scala.collection.mutable


//todo rank, name ...
case class TaxInfo(count: Int, acc: Int) {
  override def toString = count + ":" + acc
}

object taxInfoMonoid extends Monoid[TaxInfo] {
  val _unit = TaxInfo(0, 0)
  def unit: TaxInfo = _unit
  def mult(x: TaxInfo, y: TaxInfo): TaxInfo = TaxInfo(x.count + y.count, x.acc + y.acc)
}



//(sample, AssignmentType -> (tax -> taxinfo)

case class AssignTable(table: Map[(String, String), Map[String, TaxInfo]])


object assignTableMonoid extends Monoid[AssignTable] {
  val mapMonoid = new MapMonoid[(String, String), Map[String, TaxInfo]](new MapMonoid[String, TaxInfo](taxInfoMonoid))

  override def mult(x: AssignTable, y: AssignTable): AssignTable = AssignTable(mapMonoid.mult(x.table, y.table))

  override def unit: AssignTable = AssignTable(mapMonoid.unit)
}
