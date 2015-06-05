package ohnosequences.metapasta

import ohnosequences.formats.{FASTQ, RawHeader}
import ohnosequences.logging.ConsoleLogger
import ohnosequences.metapasta.databases.{GI, TaxonRetriever}
import ohnosequences.metapasta.reporting.SampleId
import org.junit.Assert._
import org.hamcrest.CoreMatchers.instanceOf


class AssignerTests {
  // val blast16s = new Blast16SFactory.BlastDatabase()

  val fakeTaxonomiTree = new MapTree(Map(
    Taxon("2") -> Taxon("1"),
    Taxon("3") -> Taxon("2"),
    Taxon("4") -> Taxon("3"),
    Taxon("4l") -> Taxon("3"),
    Taxon("5l") -> Taxon("4"),
    Taxon("5") -> Taxon("4"),
    Taxon("5r") -> Taxon("4")
  ), Taxon("1"))


  //we use fake 16s and fake taxonomy where taxid = gi
  val idGIMapper = new TaxonRetriever[GI] {

    override def getTaxon(referenceId: GI): Option[Taxon] = {
      val gi = referenceId.id
      if (fakeTaxonomiTree.map.contains(Taxon(gi)) || fakeTaxonomiTree.root.taxId.equals(gi) || gi.equals("20142015")) {
        Some(Taxon(gi))
      } else {
        None
      }
    }

  }

  //@Test
  def assignmentTest1() {
    val assignmentConfiguration = AssignmentConfiguration(100, 0.8)
    val assigner = new Assigner(
      taxonomyTree = fakeTaxonomiTree,
      taxonRetriever = idGIMapper,
      extractHeader,
      assignmentConfiguration = assignmentConfiguration,
      None
    )

    val testSample = "test"
    val chunkId = ChunkId(SampleId(testSample), 1, 1000)

    val reads = List(
      FASTQ(RawHeader("read1"), "ATG", "+", "quality"),
      FASTQ(RawHeader("read2"), "ATG", "+", "quality"),
      FASTQ(RawHeader("read3"), "ATG", "+", "quality"),
      FASTQ(RawHeader("read4"), "ATG", "+", "quality"),
      FASTQ(RawHeader("read5"), "ATG", "+", "quality"),
      FASTQ(RawHeader("read6"), "ATG", "+", "quality"),
      FASTQ(RawHeader("read7"), "ATG", "+", "quality"),
      FASTQ(RawHeader("read8"), "ATG", "+", "quality"),
      FASTQ(RawHeader("read9"), "ATG", "+", "quality"),
      FASTQ(RawHeader("read10"), "ATG", "+", "quality")
    )

    //  def refId(taxon: Taxon) = RefId("gi|" + taxon.taxId + "|gb|000|")

    val wrongRefId = "سلام"
    val refIdWithWrongGI = "20142014"
    val refIdWithWrongTaxId = "20142015"


    // def refId(taxon: Taxon) = RefId("gi|" + taxon.taxId + "|gb|000|")
    //    def refId(taxonId: String) = "gi|" + taxonId + "|gb|000|"

    val hits = List[Hit[GI]](
      //Hit("read1", "gi|5|gb|GU939576.1|", 50.5), //no hits for read1
      Hit(ReadId("read2"), GI(wrongRefId), 100), //wrong ref id
      Hit(ReadId("read3"), GI(refIdWithWrongGI), 100), //ref id is correct but isn't presented in gi mapper
      Hit(ReadId("read4"), GI(refIdWithWrongTaxId), 100), //ref id is correct but corresponded tax id isn't presented in taxonomy database
      Hit(ReadId("read5"), GI(fakeTaxonomiTree.root.taxId), 200), //one hit to root
      Hit(ReadId("read6"), GI(Taxon("2").taxId), 200) //one hit to sub root
      // Hit("read7", refId(fakeTaxonomiTree.root), 100),
      // Hit("read8", refId(fakeTaxonomiTree.root), 100),
      // Hit("read9", refId(fakeTaxonomiTree.root), 100),
      // Hit("read10", refId(fakeTaxonomiTree.root), 100),
    )

    val logger = new ConsoleLogger("assignmentTest1")
    val (table, stats) = assigner.assign(
      logger = logger,
      chunk = chunkId,
      reads = reads,
      hits = hits
    )


    //common tests
    for (assignmentType <- List(LCA, BBH)) {
      // println(stats(testSample -> assignmentType).wrongRefIds)
      assertEquals(5, stats(testSample -> assignmentType).noHit)

      assertEquals(true, stats(testSample -> assignmentType).wrongRefIds.contains(wrongRefId))
      assertEquals(true, stats(testSample -> assignmentType).wrongRefIds.contains(refIdWithWrongGI))
      assertEquals(true, stats(testSample -> assignmentType).wrongRefIds.contains(refIdWithWrongTaxId))
      assertEquals(1, table.table(testSample -> assignmentType)(fakeTaxonomiTree.root).count)
      assertEquals(2, table.table(testSample -> assignmentType)(fakeTaxonomiTree.root).acc)
      assertEquals(1, table.table(testSample -> assignmentType)(Taxon("2")).count)
    }

    //assignLCA(logger, chunk, reads, hits, assignmentConfiguration.bitscoreThreshold, assignmentConfiguration.p)


    //more specific LCA tests

    val lcaHits = List[Hit[GI]](
      //Hit("read1", "gi|5|gb|GU939576.1|", 50.5), //no hits for read1
      Hit(ReadId("read2"), GI(wrongRefId), 100.1), //wrong ref id
      Hit(ReadId("read3"), GI(refIdWithWrongGI), 100), //ref id is correct but isn't presented in gi mapper
      Hit(ReadId("read4"), GI(refIdWithWrongTaxId), 100), //ref id is correct but corresponded tax id isn't presented in taxonomy database
      Hit(ReadId("read5"), GI(fakeTaxonomiTree.root.taxId), 200), //one hit to root
      Hit(ReadId("read6"), GI(fakeTaxonomiTree.root.taxId), 10), //one hit under threshold
      Hit(ReadId("read7"), GI(fakeTaxonomiTree.root.taxId), 100),
      Hit(ReadId("read8"), GI(fakeTaxonomiTree.root.taxId), 100),
      Hit(ReadId("read9"), GI(fakeTaxonomiTree.root.taxId), 100),
      Hit(ReadId("read10"), GI(fakeTaxonomiTree.root.taxId), 100)
    )


    val (lcaAssignments, lcaStats) = new LCAAlgorithm(assignmentConfiguration).assignAll(fakeTaxonomiTree, hits, reads, assigner.getTaxIds, logger)

    assertEquals(false, lcaAssignments.contains(ReadId("read1")))
    assertThat(lcaAssignments(ReadId("read2")), instanceOf(classOf[NoTaxIdAssignment[GI]]))
    //assertEquals(NoTaxIdAssignment, lcaAssignments("read3"))
    //assertEquals(NoTaxIdAssignment, lcaAssignments("read4"))
    //assertEquals(TaxIdAssignment, lcaAssignments("read5"))
    // assertEquals(NoTaxIdAssignment, lcaAssignments("read2"))
    // assertEquals(NoTaxIdAssignment, lcaAssignments("read2"))
    // assertEquals(NoTaxIdAssignment, lcaAssignments("read2"))

  }

  def extractHeader(header: String): ReadId = ReadId(header)


}
