package ohnosequences.metapasta

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer

/**
 * a layer for bio4j taxonomy tree
 */
trait Tree[N] {
  def getParent(node: N): Option[N]
  def getRoot: N
}

class MapTree[N](map: Map[N, N], root: N) extends Tree[N] {

  override def getParent(node: N): Option[N] = map.get(node)

  override def getRoot: N = root

}

case class Taxon(taxId: String)

class Bio4JTaxonomyTree extends Tree[Taxon] {
  override def getParent(taxon: Taxon): Option[Taxon] = {
    val node = nodeRetriever.nodeRetriever.getNCBITaxonByTaxId(taxon.taxId)
    val parent = node.getParent()
    if (parent == null) {
      None
    } else {
      Some(Taxon(parent.getTaxId()))
    }
  }

  override def getRoot: Taxon = Taxon("1")
}


object TreeUtils {

  //@tailrec
  //def getParents2[N](tree: Tree[N], res: ListBuffer[N], node: N): List
  //how to do it with tail rec?

  def getParents[N](tree: Tree[N], node: N): List[N] = {
    val res = new ListBuffer[N]()
    var parent = tree.getParent(node)
    while (parent.isDefined) {
      res += parent.head
      parent = parent.flatMap(tree.getParent(_))
    }
    res.toList
  }
}



