package ohnosequences.metapasta.automatic

import org.scalacheck.{Gen, Arbitrary, Properties}
import org.scalacheck.Prop._
import ohnosequences.metapasta.{Tree, TreeUtils, MapTree}
import scala.collection.mutable
import scala.annotation.tailrec
import scala.util.Random

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

  @tailrec
  def genListAux[U, T](list: List[U], res: Gen[List[T]], gen: U => Gen[T]): Gen[List[T]] = list match {
    case Nil => res.map(_.reverse)
    case head :: tail => {
      val newRes = for {
        headN <- gen(head)
        resR <- res
      } yield headN :: resR
      genListAux(tail, newRes, gen)
    }
  }

  def genList[U, T](list: List[U], gen: U => Gen[T]): Gen[List[T]] = genListAux(list, Gen.const(List[T]()), gen)


  @tailrec
  def genMapAux[U, T](list: List[U], res: Gen[Map[U, T]], gen: U => Gen[T]): Gen[Map[U, T]] = list match {
    case Nil => res
    case head :: tail => {
      val newRes = for {
        headN <- gen(head)
        resR <- res
      } yield  resR + (head -> headN)

      genMapAux(tail, newRes, gen)
    }
  }

  def genMap[U, T](list: List[U], gen: U => Gen[T]): Gen[Map[U, T]] = genMapAux[U, T](list, Gen.const(Map[U, T]()), gen)

  def genPair[T, S](gen1: Gen[T], gen2: Gen[S]): Gen[(T, S)] = for {
    v1 <- gen1
    v2 <- gen2
  } yield (v1, v2)


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


  def tree[T](labeling: Int => T): Gen[Tree[T]] = sizedTree(labeling).map(_._1)

  def sizedTree[T](labeling: Int => T): Gen[(Tree[T], Int)] = Gen.sized { size =>
    treeRawAux(size + 1, new mutable.HashMap[T, T](), labeling).map { tree => (tree, size + 1)}
  }


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

  //todo: write note why scalacheck is crap: Gen[T] \times Gen[S] \to Gen[T \times S]
  def randomNode[T](sizedTree: Gen[(Tree[T], Int)], labeling: Int => T): Gen[(Tree[T], T)] = sizedTree.map{ case (tree: Tree[String], size) =>

    (tree, labeling(random.nextInt(size) + 1))
  }

  def randomNodePair[T](sizedTree: Gen[(Tree[T], Int)], labeling: Int => T): Gen[(Tree[T], T, T)] = sizedTree.map{ case (tree: Tree[String], size) =>
    val r = (tree, labeling(random.nextInt(size) + 1), labeling(random.nextInt(size) + 1))

    r
  }


  def randomNodeSet[T](sizedTree: Gen[(Tree[T], Int)], labeling: Int => T): Gen[(Tree[T], Set[T])] = sizedTree.map{ case (tree: Tree[String], size) =>
    val randomNodes = random.nextInt(size) + 1
    val set = (1 to randomNodes).map{n => labeling(random.nextInt(n) + 1)}.toSet
    (tree, set)
  }

  def randomNodeSets[T](sizedTree: Gen[(Tree[T], Int)], labeling: Int => T, setsBount: Int): Gen[(Tree[T], List[Set[T]])] = sizedTree.map{ case (tree: Tree[String], size) =>
    val randomNodes = random.nextInt(size) + 1
    val sets = (1 to setsBount).toList.map{_ => (1 to randomNodes).map{n => labeling(random.nextInt(n) + 1)}.toSet}
    (tree, sets)
  }

  def randomNodeList[T](sizedTree: Gen[(Tree[T], Int)], labeling: Int => T): Gen[(Tree[T], List[T])] = sizedTree.map{ case (tree: Tree[String], size) =>
    val randomNodes = random.nextInt(size) + 1
    val list = (1 to randomNodes).map{n => labeling(random.nextInt(n) + 1)}.toList
    (tree, list)
  }

}

object ScalaCheckDemo extends Properties("Tree") {
  import Generators._

  property("root") = forAll (tree(stringLabeling)) { tree =>
    tree.isNode(tree.root)
  }

  property("is node") = forAll (randomNode(sizedTree(stringLabeling), stringLabeling)) { case (tree: Tree[String], node: String) =>
    tree.isNode(node)
  }

  property("parent") = forAll (randomNode(sizedTree(stringLabeling), stringLabeling)) {  case (tree: Tree[String], node: String) =>
    if (tree.root.equals(node)) {
      tree.getParent(node).isEmpty
    } else {
      tree.getParent(node).isDefined
    }
  }

  property("lca idem") = forAll (randomNode(sizedTree(stringLabeling), stringLabeling)) { case (tree: Tree[String], node: String) =>
    val lca = TreeUtils.lca(tree, node, node)
    node.equals(lca)
  }

  property("lca shuffle") = forAll (randomNodeList(sizedTree(stringLabeling), stringLabeling)) { case (tree: Tree[String], nodes: List[String]) =>
    val lca1 = TreeUtils.lca(tree, nodes)
    val lca2 = TreeUtils.lca(tree, random.shuffle(nodes))
    lca1.equals(lca2)
  }

  property("in line") = forAll (randomNodeSet(sizedTree(stringLabeling), stringLabeling)) { case (tree: Tree[String], nodes: Set[String]) =>
    TreeUtils.isInLine(tree, nodes) match {
      case None => {
        true //check something
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
