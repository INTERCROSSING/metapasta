package ohnosequences.metapasta

import ohnosequences.nisperon.Monoid
import scala.collection.mutable


class ReadsStatsBuilder() {

  var merged = 0
  var notMerged = 0 //problems with flash
  var noHit = 0 //no hit: too strong mapping parameters
  var notAssigned = 0 //thresholds are to strict, in some cases (best blast hit) it can be due to wrong refs
  var wrongRefIds = new mutable.HashSet[String]() //all wrong refs are ignored



  def incrementMerged() = {merged += 1}
 // def incrementssigned() = {assigned += 1}
  def incrementNotAssigned() = {notAssigned += 1}
  def incrementNotMerged() = {notMerged += 1}
  def incrementNoHit() = {noHit += 1}

  def addWrongRefId(id: String) = {wrongRefIds += id}


  def build = ReadsStats(
    merged = merged,
    notMerged = notMerged,
    noHit = noHit,
    notAssigned = notAssigned,
    wrongRefIds = wrongRefIds.toSet
  )
}

case class ReadsStats(merged: Int = 0,
                      notMerged: Int = 0,
                      noHit: Int = 0,
                      notAssigned: Int = 0,
                      wrongRefIds: Set[String]) {
  def mult(y: ReadsStats): ReadsStats = readsStatsMonoid.mult(this, y)
}

object readsStatsMonoid extends Monoid[ReadsStats] {

  override def mult(x: ReadsStats, y: ReadsStats): ReadsStats = {
    ReadsStats(
      merged = x.merged + y.merged,
      notMerged = x.notMerged + y.notMerged,
      noHit = x.noHit + y.noHit,
      notAssigned = x.notAssigned + y.notAssigned,
      wrongRefIds = x.wrongRefIds ++ y.wrongRefIds
    )
  }

  val _unit = ReadsStats(0, 0, 0, 0, Set[String]())
  override def unit: ReadsStats = _unit
}
