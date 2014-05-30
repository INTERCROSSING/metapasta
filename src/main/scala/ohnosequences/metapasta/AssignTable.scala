package ohnosequences.metapasta

import ohnosequences.nisperon.Monoid
import scala.collection.mutable


//todo rank, name ...
case class TaxInfo(count: Int, acc: Int) {
  override def toString = count + ":" + acc
}

object TaxInfoMonoid extends Monoid[TaxInfo] {
  val _unit = TaxInfo(0, 0)

  def unit: TaxInfo = _unit

  def mult(x: TaxInfo, y: TaxInfo): TaxInfo = TaxInfo(x.count + y.count, x.acc + y.acc)
}



//sample -> (tax -> taxinfo)

case class  AssignTable(table: Map[String, Map[String, TaxInfo]])


object AssignTableMonoid extends Monoid[AssignTable] {
  val _unit = AssignTable(Map[String, Map[String, TaxInfo]]())
  def unit: AssignTable = _unit

  def mult(x: AssignTable, y: AssignTable): AssignTable = {
    val preRes = mutable.HashMap[String, Map[String, TaxInfo]]()

    for (sample <- x.table.keySet ++ y.table.keySet) {

      val prepreRes = mutable.HashMap[String, TaxInfo]()

      x.table.getOrElse(sample,  Map[String, TaxInfo]()).foreach { case (tax, taxInfo) =>
        prepreRes.get(tax) match {
          case None => prepreRes.put(tax, taxInfo)
          case Some(taxInfo2) => prepreRes.put(tax, TaxInfoMonoid.mult(taxInfo, taxInfo2))
        }
      }

      y.table.getOrElse(sample, Map[String, TaxInfo]()).foreach { case (tax, taxInfo) =>
        prepreRes.get(tax) match {
          case None => prepreRes.put(tax, taxInfo)
          case Some(taxInfo2) => prepreRes.put(tax, TaxInfoMonoid.mult(taxInfo, taxInfo2))
        }
      }
      preRes.put(sample, prepreRes.toMap)
    }
    AssignTable(preRes.toMap)

  }
}