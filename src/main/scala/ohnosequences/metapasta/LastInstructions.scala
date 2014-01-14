package ohnosequences.metapasta

import ohnosequences.nisperon.{MapInstructions, AWS}
import ohnosequences.awstools.s3.ObjectAddress
import java.io.{PrintWriter, File}
import org.clapper.avsl.Logger


class LastInstructions(aws: AWS, database: Database) extends MapInstructions[List[ParsedSampleChunk], List[BlastResult]] {

  val logger = Logger(this.getClass)


  override def prepare() {

    logger.info("installing database")
    database.install()

    logger.info("downloading LAST")
    val last = ObjectAddress("metapasta", "lastal")
    val lm = aws.s3.createLoadingManager()

    val f = new File("lastal")
    lm.download(last, f)
    f.setExecutable(true)
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

  def apply(input: List[ParsedSampleChunk]): List[BlastResult] = {

    import scala.sys.process._

    val chunk = input.head

    logger.info("saving reads to reads.fastq")
    writeFile(chunk.toFasta, new File("reads.fastq"))


    logger.info("running LAST")
    val command =  """./lastal $name$ reads.fastq -s 2 -T 1 -f 0 -r5 -q95 -a0 -b95 -e400 -Q2 > out.last.maf"""
      .replace("$name$", database.name)
   // val command = """blastn -task megablast -db $name$ -query reads.fasta -out result -max_target_seqs 1 -num_threads 1 -outfmt 6 -show_gis"""
   //   .replace("$name$", database.name)

    val startTime = System.currentTimeMillis()
    val code = command.!
    val endTime = System.currentTimeMillis()

    logger.info("last: " + (endTime - startTime + 0.0) / chunk.fastqs.size + " ms per read")

    if(code != 0) {
      throw new Error("LAST finished with error code " + code)
    }

    logger.info("reading BLAST result")
    val resultRaw = readFile(new File("out.last.maf"))

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
