import ohnosequences.formats.{RawHeader, FASTQ}
import ohnosequences.metapasta._
import ohnosequences.metapasta.AssignmentConfiguration
import ohnosequences.metapasta.databases.{GIMapper, Blast16SFactory}
import ohnosequences.metapasta.databases.Blast16SFactory.BlastDatabase
import ohnosequences.metapasta.reporting.SampleId
import ohnosequences.metapasta.Taxon
import ohnosequences.nisperon.logging.ConsoleLogger
import org.junit.Test
import org.junit.Assert._
import scala.Some

class AssignerTests {
  val blast16s = new Blast16SFactory.BlastDatabase()

  val fakeTaxonomiTree = new MapTree(Map(
    Taxon("2") -> Taxon("1"),
    Taxon("3") -> Taxon("2"),
    Taxon("4") -> Taxon("3"),
    Taxon("4l") -> Taxon("3"),
    Taxon("5l") -> Taxon("4"),
    Taxon("5") -> Taxon("4"),
    Taxon("5r") -> Taxon("4")
  ), Taxon("1"))

  val idGIMapper = new GIMapper {
    override def getTaxIdByGi(gi: String): Option[String] = {
      Some(gi)
    }
  }

  def extractHeader(header: String) = header

  @Test
  def assignmentTest1() {
    val assignmentConfiguration = AssignmentConfiguration(50, 0.8)
    val assigner = new Assigner(
      taxonomyTree = fakeTaxonomiTree,
      database = blast16s,
      giMapper = idGIMapper,
      assignmentConfiguration = assignmentConfiguration,
      extractHeader,
      None
    )

    val testSample = "test"
    val chunkId = ChunkId(SampleId(testSample), 1, 1000)

    val reads = List(
      FASTQ(RawHeader("read1"), "ATG", "+", "quality"),
      FASTQ(RawHeader("read2"), "ATG", "+", "quality"),
      FASTQ(RawHeader("read3"), "ATG", "+", "quality")
    )

    val hits = List[Hit](
      Hit("read1", "|gi5|gbssss", 50.5)
    )

    val (table, stats) = assigner.assign(
      logger = new ConsoleLogger("test"),
      chunk = chunkId,
      reads = reads,
      hits = hits
    )

    assertEquals(2, stats(testSample -> LCA).noHit)
  }


}
