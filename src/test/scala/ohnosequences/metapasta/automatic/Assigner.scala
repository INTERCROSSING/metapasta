package ohnosequences.metapasta.automatic

import org.scalacheck.{Gen, Properties}
import scala.util.Random

import ohnosequences.metapasta._
import org.scalacheck.Prop._
import ohnosequences.metapasta.Hit
import ohnosequences.formats.{RawHeader, FASTQ}


object HitsGenerator {
  import Generators.{genList, genMap}

  val random = new Random()

  def hitsPerReadId(readId: Int, treeSize: Int, labeling: Int => String): Gen[List[Hit]] = {
    Gen.listOf(Gen.choose(1, treeSize).map(labeling).flatMap(hit(readId, _)))
  }

  def hit(readId: Int, node: String): Gen[Hit] = {
    for {
      quality <- Gen.chooseNum(0, 200)
    } yield Hit(ReadId(readId.toString), RefId(node), quality)
  }


  def grouppedHits(readsAmount: Int, treeSize: Int, labeling: Int => String): Gen[Map[Int, List[Hit]]] = {
    genMap((1 to readsAmount).toList, { id => hitsPerReadId(id, treeSize, labeling)})   
  }

  def read(readId: Int) = FASTQ(RawHeader(readId.toString), "ATG", "+", "quality")




}

object Assigner  extends Properties("Assigner") {

  import Generators._

  property("genList smaller test") = forAll (Gen.listOfN(10, Gen.choose(1, 100)).flatMap { list => genPair(Gen.const(list), genList[Int, Int](list, {n: Int => Gen.choose(1, n)}))}) { case (list1, list2) =>
    list1.zip(list2).forall { case (x, y) => x >= y}
  }

  property("genMap smaller test") = forAll (Gen.listOfN(10, Gen.choose(1, 100)).flatMap { list => genMap[Int, Int](list, {n: Int => Gen.choose(1, n)})}) { case map =>
    map.forall{ case (key, value) => key >= value}
  }

  property("hits general") = forAll (HitsGenerator.grouppedHits(10, 100, stringLabeling)) { case map =>
    (1 to 10).forall(map.contains(_))
  }

  property("hits nodes") = forAll (HitsGenerator.grouppedHits(10, 100, stringLabeling)) { case map =>
    (1 to 10).forall { readId =>
      map(readId).forall { hit =>
        hit.readId.readId.toInt.equals(readId) && (hit.refId.refId.toInt <= 100)
      }
    }
  }



//  property("assign one") = forAll (HitsGenerator.grouppedHits(10, 100, stringLabeling), sizedTree()) { case map =>
//
//    val assigner = new Assigner(
//      taxonomyTree = fakeTaxonomiTree,
//      database = blast16s,
//      giMapper = idGIMapper,
//      assignmentConfiguration = assignmentConfiguration,
//      extractHeader,
//      None
//    )
//
//    val reads  = map.keys.toList.map(HitsGenerator.read(_))
//    val hits: List[Hit] = map.values.toList.flatMap {list => list}
//
//
//    (1 to 10).forall { readId =>
//      map(readId).forall { hit =>
//        hit.readId.readId.toInt.equals(readId) && (hit.refId.refId.toInt <= 100)
//      }
//    }
//  }


//  property("assigner amount") = forAll (randomNodeSets(sizedTree(stringLabeling), stringLabeling, 100), Gen.choose(1, 100)) { case ((tree, sets), readAmount) =>
//
//    val reads  =
//
//    val hits: List[Hit] = (1 to 100).toList.map{ i =>
//      new Hit(ReadId("read" + random.nextInt(readAmount).toString), RefId(random.nextInt(25).toString), 100)
//    }
//
//    val lca = TreeUtils.lca(tree, node, node)
//    node.equals(lca)
//  }


}
