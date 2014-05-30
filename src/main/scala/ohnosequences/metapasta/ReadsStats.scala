package ohnosequences.metapasta

import ohnosequences.nisperon.Monoid


case class ReadsStats(assigned: Int, unassigned: Int, unknownGI: Int, unknownTaxId: Int, unmerged: Int) {}

object readsStatsMonoid extends Monoid[ReadsStats] {

  override def mult(x: ReadsStats, y: ReadsStats): ReadsStats = {
    ReadsStats(
      assigned = x.assigned + y.assigned,
      unassigned = x.unassigned + y.unassigned,
      unknownGI = x.unknownGI + y.unknownGI,
      unknownTaxId = x.unknownTaxId + y.unknownTaxId,
      unmerged = x.unmerged + y.unmerged
    )
  }

  val _unit = ReadsStats(0, 0, 0, 0, 0)
  override def unit: ReadsStats = _unit
}
