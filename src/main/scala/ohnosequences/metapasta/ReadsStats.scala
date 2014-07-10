package ohnosequences.metapasta

import ohnosequences.nisperon.{JsonSerializer, Serializer, Monoid}
import scala.collection.mutable


class ReadsStatsBuilder() {

  var total = 0L
  var merged = 0L
  var notMerged = 0L //problems with flash
  var noHit = 0L //no hit: too strong mapping parameters
  var notAssigned = 0L //thresholds are to strict, in some cases (best blast hit) it can be due to wrong refs
  var assigned = 0L
  var wrongRefIds = new mutable.HashSet[String]() //all wrong refs are ignored


  def incrementTotal() = {total += 1}
  def incrementMerged() = {merged += 1}
  def incrementNotAssigned() = {notAssigned += 1}
  def incrementNotMerged() = {notMerged += 1}
  def incrementNoHit() = {noHit += 1}
  def incrementAssigned() = {assigned += 1}

  def addWrongRefId(id: String) = {wrongRefIds += id}


  def build = ReadsStats(
    total = total,
    merged = merged,
    notMerged = notMerged,
    noHit = noHit,
    notAssigned = notAssigned,
    assigned = assigned,
    wrongRefIds = wrongRefIds.toSet
  )
}

case class ReadsStats(total: Long,
                      merged: Long,
                      notMerged: Long,
                      noHit: Long,
                      notAssigned: Long,
                      assigned: Long,
                      wrongRefIds: Set[String] = Set[String]()) {
  def mult(y: ReadsStats): ReadsStats = readsStatsMonoid.mult(this, y)
}


//todo should be on samples level + e-mail generation
object readsStatsMonoid extends Monoid[ReadsStats] {

  override def mult(x: ReadsStats, y: ReadsStats): ReadsStats = {
    ReadsStats(
      total = x.total + y.total,
      merged = x.merged + y.merged,
      notMerged = x.notMerged + y.notMerged,
      noHit = x.noHit + y.noHit,
      notAssigned = x.notAssigned + y.notAssigned,
      assigned = x.assigned + y.assigned,
      wrongRefIds = x.wrongRefIds ++ y.wrongRefIds
    )
  }

  val _unit = ReadsStats(0, 0, 0, 0, 0, 0, Set[String]())
  override def unit: ReadsStats = _unit
}

object readsStatsSerializer extends Serializer[Map[(String, AssignmentType), ReadsStats]] {

  val rawStatsSerializer = new JsonSerializer[Map[String, ReadsStats]]()
  override def toString(t: Map[(String, AssignmentType), ReadsStats]): String = {
    val raw: Map[String, ReadsStats] = t.map { case (sampleAssignmentType, stats)  =>
      (sampleAssignmentType._1 + "###" + sampleAssignmentType._2.toString, stats)
    }
    rawStatsSerializer.toString(raw)
  }

  override def fromString(s: String): Map[(String, AssignmentType), ReadsStats] = {
    val raw : Map[String, ReadsStats]= rawStatsSerializer.fromString(s)
    raw.map { case (sampleAssignmentType, stats)  =>
      val parts = sampleAssignmentType.split("###")
      ((parts(0), AssignmentType.fromString(parts(1))), stats)
    }
  }
}
