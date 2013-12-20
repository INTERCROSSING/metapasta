package ohnosequences.metapasta

import ohnosequences.nisperon.{AWS, MapInstructions}
import java.io.{PrintWriter, File}
import ohnosequences.awstools.s3.ObjectAddress
import org.clapper.avsl.Logger
import scala.collection.mutable.ListBuffer

case class BlastResult(readId: String, gi: String)

class BlastInstructions(aws: AWS, database: Database) extends MapInstructions[String, List[BlastResult]] {

  val logger = Logger(this.getClass)

  override def prepare() {
    import scala.sys.process._

    logger.info("installing database")
    database.install()

    logger.info("downloading BLAST")
    val blast = ObjectAddress("resources.ohnosequences.com", "blast/ncbi-blast-2.2.25.tar.gz")
    val lm = aws.s3.createLoadingManager()
    lm.download(blast, new File("ncbi-blast-2.2.25.tar.gz"))

    logger.info("extracting BLAST")
    """tar -xvf ncbi-blast-2.2.25.tar.gz""".!

    logger.info("installing BLAST")
   // new Runtime().exec()
    Seq("sh", "-c", """cp ./ncbi-blast-2.2.25+/bin/* /usr/bin""").!
   // Runtime.getRuntime.exec("""cp ./ncbi-blast-2.2.25+/bin/* /usr/bin""").exitValue()

  }

  def writeFile(s: String, file: File) {
    val writer = new PrintWriter(file)
    writer.print(s)
    writer.close()
  }

  def readFile(file: File): String = {
    scala.io.Source.fromFile(file).mkString
  }

  def apply(input: String): List[BlastResult] = {
    import scala.sys.process._

    logger.info("saving reads to reads.fasta")
    writeFile(input, new File("reads.fasta"))


    logger.info("running BLAST")
    val command = """blastn -task megablast -db $name$ -query reads.fasta -out result -max_target_seqs 1 -num_threads 1 -outfmt 6 -show_gis"""
      .replace("$name$", database.name)

    val code = command.!

    if(code != 0) {
      throw new Error("BLAST finished with error code " + code)
    }

    logger.info("reading BLAST result")
    val resultRaw = readFile(new File("result"))

    logger.info("parsing BLAST result")
    val result = ListBuffer[BlastResult]()
    //todo reads without hits!!!
    //M00476_38_000000000_A3FHW_1_1101_20604_2554_1_N_0_28	gi|313494140|gb|GU939576.1|	99.21	253	2	0	1	253	362	614	3e-127	 457
    val hit = """\s*([^\s]+)\s+([^\s]+)\s+([^\s]+).+""".r
    resultRaw.linesIterator.foreach {
      case hit(readId, refId, score) => result += BlastResult(readId, database.parseGI(refId))
      case l => logger.error("can't parse: " + l)
    }

    result.toList
  }
}
