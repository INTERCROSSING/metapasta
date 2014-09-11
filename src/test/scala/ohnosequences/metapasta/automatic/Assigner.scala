package ohnosequences.metapasta.automatic

import ohnosequences.metapasta.databases.{GIMapper, Blast16SFactory}
import ohnosequences.metapasta.reporting.SampleId
import ohnosequences.nisperon.logging.ConsoleLogger
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
      quality <- Gen.chooseNum(300, 400)
    } yield Hit(ReadId(readId.toString), refId(Taxon(node)), quality)
  }

  def refId(taxon: Taxon) = RefId("gi|" + taxon.taxId + "|gb|000|")

  def groupedHits(readsAmount: Int, treeSize: Int, labeling: Int => String): Gen[Map[Int, List[Hit]]] = {
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

  property("hits general") = forAll (HitsGenerator.groupedHits(10, 100, stringLabeling)) { case map =>
    (1 to 10).forall(map.contains(_))
  }

  property("hits nodes") = forAll (HitsGenerator.groupedHits(10, 100, stringLabeling)) { case map =>
    (1 to 10).forall { readId =>
      map(readId).forall { hit =>
        hit.readId.readId.toInt.equals(readId)
      }
    }
  }


  val blast16s = new Blast16SFactory.BlastDatabase()

  def extractHeader(header: String) = header


  property("assign check stats") = forAll (sizedTree(stringLabeling).flatMap { case (tree, treeSize) =>
    val groupedHits = Gen.choose(1, 100).flatMap(HitsGenerator.groupedHits(_, treeSize, stringLabeling))
    groupedHits.map { hits => (tree, treeSize, hits)}
  }, Gen.oneOf(LCA, BBH)) { case ((tree, treeSize, groupedHits), assignmentType) =>

    val randomTaxonomyTree: Tree[Taxon] = Tree.relabel(tree, {s: String => Taxon(s)}, {tax: Taxon => tax.taxId})

    //println("tree_size: " + treeSize + " parent(Taxon(10)):" + randomTaxonomyTree.getParent(Taxon("10")))

    val idGIMapper = new GIMapper {
      override def getTaxIdByGi(gi: String): Option[Taxon] = {
        if (randomTaxonomyTree.isNode(Taxon(gi))) {
          Some(Taxon(gi))
        } else {
          None
        }
      }
    }

    val assignmentConfiguration  = AssignmentConfiguration(100, 0.8)

    val assigner = new Assigner(
      taxonomyTree = randomTaxonomyTree,
      database = blast16s,
      giMapper = idGIMapper,
      assignmentConfiguration = assignmentConfiguration,
      extractReadHeader = extractHeader,
      None
    )

    val reads  = groupedHits.keys.toList.map(HitsGenerator.read(_))
    val hits: List[Hit] = groupedHits.values.toList.flatMap {list => list}

    val logger = new ConsoleLogger("assign one", verbose = false)
    val testSample = "test"
    val chunkId = ChunkId(SampleId(testSample), 1, 1000)

    val (tables, map) = assigner.assign(
      logger = logger,
      chunk = chunkId,
      reads = reads,
      hits = hits
    )

    val sampleAndAssignmentType = testSample -> assignmentType
    val stats = map(sampleAndAssignmentType)
    val table = tables.table(sampleAndAssignmentType)

   // println(assignmentType + "stats: " + stats + " reads: " + reads.size + " tree_size: " + treeSize)

    (stats.noHit + stats.notAssigned + stats.noTaxId + stats.assigned == reads.size) :| "all reads tracked" &&
    (stats.lcaAssigned + stats.lineAssigned  + stats.bbhAssigned == stats.assigned) :| "all reads tracked" &&
    (table(NotAssignedCat.taxon).acc == stats.notAssigned) :| "not assigned value test 1" &&
    (table(NotAssignedCat.taxon).count == stats.notAssigned) :| "not assigned value test 2" &&
    (table(NoTaxId.taxon).acc == stats.noTaxId) :| "not tax id value test 1" &&
    (table(NoTaxId.taxon).count == stats.noTaxId) :| "not tax id value test 2" &&
    (table(NoHit.taxon).acc == stats.noHit) :| "no hit value test 1" &&
    (table(NoHit.taxon).count == stats.noHit) :| "no hit value test 2"

  }


  property("assign lca") = forAll (sizedTree(stringLabeling).flatMap { case (tree, treeSize) =>
    val gropedHits = Gen.choose(1, 100).flatMap(HitsGenerator.groupedHits(_, treeSize, stringLabeling))
    gropedHits.map { hits => (tree, treeSize, hits)}
  }) { case (tree, treeSize, gropedHits) =>

    val randomTaxonomyTree: Tree[Taxon] = Tree.relabel(tree, {s: String => Taxon(s)}, {tax: Taxon => tax.taxId})

    //println("tree_size: " + treeSize + " parent(Taxon(10)):" + randomTaxonomyTree.getParent(Taxon("10")))

    val idGIMapper = new GIMapper {
      override def getTaxIdByGi(gi: String): Option[Taxon] = {
        if (randomTaxonomyTree.isNode(Taxon(gi))) {
          Some(Taxon(gi))
        } else {
          None
        }
      }
    }

    val assignmentConfiguration  = AssignmentConfiguration(100, 0.8)

    val assigner = new Assigner(
      taxonomyTree = randomTaxonomyTree,
      database = blast16s,
      giMapper = idGIMapper,
      assignmentConfiguration = assignmentConfiguration,
      extractReadHeader = extractHeader,
      None
    )

    val reads  = gropedHits.keys.toList.map(HitsGenerator.read(_))
    val hits: List[Hit] = gropedHits.values.toList.flatMap {list => list}

    val logger = new ConsoleLogger("assign one", verbose = false)
    val testSample = "test"
    val chunkId = ChunkId(SampleId(testSample), 1, 1000)

    val (assignments, stats0) = assigner.assignLCA(
      logger = logger,
      chunk = chunkId,
      reads = reads,
      hits = hits
    )

    val (table, stats) = assigner.prepareAssignedResults(
      logger = logger,
      chunk = chunkId,
      assignmentType = LCA,
      reads = reads,
      assignment = assignments,
      initialReadsStats = stats0
    )

    val oneLineAssignments = assignments.flatMap {
      case (readId, assignment: TaxIdAssignment) if assignment.line => Some((readId, assignment))
      case _ => None
    }

    val lcaAssignments = assignments.filter {
      case (readId, assignment: TaxIdAssignment) if assignment.lca => true
      case _ => false
    }

    val bestScores = AssignerAlgorithms.bestScores(hits)


    (oneLineAssignments.size == stats.lineAssigned) :| "one line amount check" &&
    (lcaAssignments.size == stats.lcaAssigned) :| "one line amount check" &&
    (oneLineAssignments.forall { case (read, assignment) =>
      val readHits: List[Hit]  = hits.filter(_.readId.equals(read))
      val filteredHits = readHits.filter { hit => AssignerAlgorithms.filterHit(hit, bestScores, assignmentConfiguration, logger)}
      val taxaSet = assigner.getTaxIds(filteredHits, logger)._1.map(_._2).toSet
      //check that all tax in lineage of most specific
      TreeUtils.getLineage(randomTaxonomyTree, assignment.taxon).takeRight(taxaSet.size).toSet.equals(taxaSet)
    })  :| "check one line taxa"

  }
}
