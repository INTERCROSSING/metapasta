package ohnosequences.metapasta.automatic

import org.scalacheck.{Gen, Arbitrary, Properties}
import org.scalacheck.Prop._
import ohnosequences.metapasta.{Tree, TreeUtils, MapTree}
import scala.collection.mutable
import scala.annotation.tailrec
import scala.util.Random



object TreeTests extends Properties("Tree") {
  import Generators._

  property("root") = forAll (boundedTree(stringLabeling, 1000)) { case (tree, size) =>
    tree.isNode(tree.root)

  }

  property("is node") = forAll (randomNode(boundedTree(stringLabeling, 1000), stringLabeling)) { case (tree, node) =>
  // println(">> tree=" + tree + " size=" + node)
    tree.isNode(node)
  }

  property("parent") = forAll (randomNode(boundedTree(stringLabeling, 1000), stringLabeling)) {  case (tree, node) =>
    if (tree.root.equals(node)) {
      tree.getParent(node).isEmpty
    } else {
      tree.getParent(node).isDefined
    }
  }

  property("lca idem") = forAll (randomNode(boundedTree(stringLabeling, 1000), stringLabeling)) { case (tree, node) =>
    val lca = TreeUtils.lca(tree, node, node)
    node.equals(lca)
  }

  property("lca shuffle") = forAll (randomNodeList(boundedTree(stringLabeling, 1000), stringLabeling)) { case (tree, nodes) =>
    val lca1 = TreeUtils.lca(tree, nodes.toSet)
    val lca2 = TreeUtils.lca(tree, random.shuffle(nodes.toSet))
    lca1.equals(lca2)
  }

  property("in line") = forAll (randomNodeSet(boundedTree(stringLabeling, 1000), stringLabeling)) { case (tree, nodes) =>
    TreeUtils.isInLine(tree, nodes) match {
      case None => {
        nodes.forall { node =>
          !TreeUtils.getLineage(tree, node).take(nodes.size).toSet.equals(nodes)
        }
        true
      }
      case Some(node) => {
        //println("are in line, tree: " + tree.toString + " nodes:" + nodes)
        TreeUtils.getLineage(tree, node).take(nodes.size).toSet.equals(nodes)
      }
    }
  }

  property("sum") = forAll (Generators.partitions(10)) { l: List[Int] =>
    l.sum == 10
  }


}
