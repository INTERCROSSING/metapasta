package ohnosequences.metapasta.automatic

import org.scalacheck.{Arbitrary, Gen, Properties}
import org.scalacheck.Prop._
import ohnosequences.metapasta.{TreeUtils, MapTree, Tree}
import scala.collection.mutable
import scala.annotation.tailrec
import scala.util.Random
import scala.Some

object Generators {



  @tailrec
  def repeat[T](gen: Gen[T], attempt: Int = 10): Option[T] = {
    gen.sample match {
      case Some(s) => Some(s)
      case None => {
        if (attempt == 0) {
          None
        } else {
          repeat(gen, attempt - 1)
        }
      }
    }
  }


  val random = new Random()

  @tailrec
  def treeRawAux[T](size: Int, map: mutable.HashMap[T, T], labeling: Int => T): Gen[MapTree[T]] = {
    if (size == 1) {
      Gen.const(new MapTree[T](map.toMap, labeling(1)))
    } else {
      map.put(labeling(size), labeling(random.nextInt(size - 1) + 1))
      treeRawAux[T](size - 1, map, labeling)
    }
  }

  def intLabeling(i: Int) = i
  def stringLabeling(i: Int) = i.toString

  def tree[T](size: Int, labeling: Int => T): Gen[Tree[T]] = treeRawAux(size, new mutable.HashMap[T, T](), labeling)

  def partitions(size: Int): Gen[List[Int]] = {

    Gen.listOfN(size, Arbitrary.arbBool).map {
      cuts =>
        var res = List[Int]()
        var lastCut = 0
        var pos = 0

        for (cut <- cuts) {
          cut.arbitrary.map {
            cutVal =>
              if (cutVal) {
                res = (pos - lastCut) :: res
                lastCut = pos
              }
          }
          pos += 1
        }
        res = (pos - lastCut) :: res
        lastCut = pos
        res
    }
  }

//  def tree[T](size: Int, gen: Gen[T]): Gen[Tree[T]] = {
//
//    val elements = new mutable.HashMap[Int, T]()
//    @tailrec
//    def fill(i: Int): Boolean = {
//      gen.
//    }
//
//
//    for ( i <- 1 to size) {
//      elements.put(i, gen.sample)
//    }
//
//    var allowed =
//  }
// too slow
//  case class CaseTree[T](root: T, subtrees: List[Tree[T]]) extends Tree[T] {
//
//    val tree = root
//
//    override def isNode(node: T): Boolean = {
//      if (root.equals(node)) {
//        true
//      } else {
//        subtrees.exists(_.isNode(node))
//      }
//    }
//
//    override def getParent(node: T): Option[T] = {
//      if (root.equals(node)) {
//        None
//      } else {
//        subtrees.fin
//      }
//    }
//  }
//
//  def tree[T](size: Int, gen: Gen[T]): Gen[Tree[T]] = {
//    match {}
//  }
}

object ScalaCheckDemo extends Properties("Tree") {


  property("root") = forAll (Generators.tree(10, Generators.stringLabeling)) { tree: Tree[String] =>
    tree.isNode(tree.root)
  }

  property("is node") = forAll (Generators.tree(10, Generators.stringLabeling), Gen.choose(1,10)) { case (tree: Tree[String], n: Int) =>
    tree.isNode(Generators.stringLabeling(n))
  }

  property("parent") = forAll (Generators.tree(10, Generators.stringLabeling), Gen.choose(1,10)) { case (tree: Tree[String], n: Int) =>
   val node = Generators.stringLabeling(n)
    if (tree.root.equals(node)) {
      tree.getParent(node).isEmpty
    } else {
      tree.getParent(node).isDefined
    }
  }

  property("lca min") = forAll (Generators.tree(10, Generators.stringLabeling), Gen.choose(1,10), Gen.choose(1,10)) { case (tree: Tree[String], n: Int, m: Int) =>
    val node1 = Generators.stringLabeling(n)
    val node2 = Generators.stringLabeling(n)
    val lca = TreeUtils.lca(tree, node1, node2)
    node1.equals(lca) || node2.equals(lca)
  }

  property("lca shuffle") = forAll (Generators.tree(10, Generators.stringLabeling), Gen.choose(1,10), Gen.choose(1,10), Gen.choose(1,10)) { case (tree: Tree[String], n1: Int, n2: Int, n3: Int) =>
    val nodes = List(Generators.stringLabeling(n1), Generators.stringLabeling(n2), Generators.stringLabeling(n3))
    val lca1 = TreeUtils.lca(tree, nodes)
    val lca2 = TreeUtils.lca(tree, Generators.random.shuffle(nodes))
    lca1.equals(lca2)
  }

  property("in line") = forAll (Generators.tree(20, Generators.stringLabeling), Gen.listOfN(5, Gen.choose(1,20))) { case (tree: Tree[String], list: List[Int]) =>
    val nodes = list.map(Generators.stringLabeling).toSet
    TreeUtils.isInLine(tree, nodes) match {
      case None => {
        true //check something
      }
      case Some(node) => {
        TreeUtils.getLineage(tree, node).take(nodes.size).toSet.equals(nodes)
      }
    }
  }

  property("sum") = forAll (Generators.partitions(10)) { l: List[Int] =>
    l.sum == 10
  }


}
