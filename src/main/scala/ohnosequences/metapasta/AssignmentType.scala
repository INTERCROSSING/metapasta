//todo doesn't work with json4s
package ohnosequences.metapasta
//
//
sealed trait AssignmentType
//
object LCA extends AssignmentType
//
object BBH extends AssignmentType

object AssignmentType {
  val LCA = "LCA"
  val BBH = "BBH"

  def fromString(s: String): AssignmentType = {
    s match {
      case LCA => ohnosequences.metapasta.LCA
      case BBH => ohnosequences.metapasta.BBH
    }
  }
}



