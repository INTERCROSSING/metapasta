package ohnosequences.metapasta

import ohnosequences.nisperon.Monoid


class ReadsStatsBuilder() {
  var merged = 0
  var assigned = 0
  var unassigned = 0
  var unknownGI = 0
  var unknownRefId = 0
  var unmerged = 0
  var lcaFiltered = 0

  def incrementMerged() = {merged += 1}
  def incrementAssigned() = {assigned += 1}
  def incrementUnassigned() = {unassigned += 1}
  def incrementUnknownGI() = {unknownGI += 1}
  def incrementUnknownRefId() = {unknownRefId += 1}
  def incrementUnmerged() = {unmerged += 1}
  def incrementLCAFiltered() = {lcaFiltered += 1}

  def build = ReadsStats(
    merged = merged,
    assigned = assigned,
    unassigned = unassigned,
    unknownGI = unknownGI,
    unknownRefId = unknownRefId,
    unmerged = unmerged,
    lcaFiltered=  lcaFiltered
  )
}

case class ReadsStats(merged: Int, assigned: Int, unassigned: Int, unknownGI: Int, unknownRefId: Int, unmerged: Int, lcaFiltered: Int) {
  def mult(y: ReadsStats): ReadsStats = readsStatsMonoid.mult(this, y)
}

object readsStatsMonoid extends Monoid[ReadsStats] {

  override def mult(x: ReadsStats, y: ReadsStats): ReadsStats = {
    ReadsStats(
      merged = x.merged + y.merged,
      assigned = x.assigned + y.assigned,
      unassigned = x.unassigned + y.unassigned,
      unknownGI = x.unknownGI + y.unknownGI,
      unknownRefId = x.unknownRefId + y.unknownRefId,
      unmerged = x.unmerged + y.unmerged,
      lcaFiltered = x.lcaFiltered + y.lcaFiltered
    )
  }

  val _unit = ReadsStats(0, 0, 0, 0, 0, 0, 0)
  override def unit: ReadsStats = _unit
}
