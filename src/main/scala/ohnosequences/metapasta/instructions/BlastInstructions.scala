package ohnosequences.metapasta.instructions

import ohnosequences.nisperon._
import org.clapper.avsl.Logger
import java.io.{File, PrintWriter}
import scala.collection.mutable.ListBuffer
import ohnosequences.nisperon.logging.S3Logger
import ohnosequences.metapasta._
import ohnosequences.metapasta.databases.{DatabaseFactory, InMemoryGIMapperFactory, BlastDatabase16S}
import ohnosequences.nisperon.bundles.NisperonMetadataBuilder
import ohnosequences.metapasta.MergedSampleChunk
import ohnosequences.parsers.S3ChunksReader
import ohnosequences.formats.RawHeader
import ohnosequences.formats.FASTQ
import ohnosequences.metapasta.AssignTable


class BlastInstructions(
                       aws: AWS,
                       metadataBuilder: NisperonMetadataBuilder, //for bio4j
                       assignmentConfiguration: AssignmentConfiguration,
                       databaseFactory: DatabaseFactory[BlastDatabase16S],
                       blastCommandTemplate: String = """blastn -task megablast -db $db$ -query $input$ -out $output$ -max_target_seqs 1 -num_threads 1 -outfmt 6 -show_gis""",
                       useXML: Boolean,
                       logging: Boolean
                       ) extends
   MapInstructions[List[MergedSampleChunk],  (Map[String, AssignTable], Map[String, ReadsStats])] {


  case class BlastContext(nodeRetriever: NodeRetriever, database: BlastDatabase16S, blast: Blast, assigner: Assigner)
  override type Context = BlastContext

  val logger = Logger(this.getClass)

  //todo think about this space
  def extractHeader(s: String) = s.replace("@", "").split("\\s")(0)


  override def prepare() = {
    val lm = aws.s3.createLoadingManager()
    val nodeRetreiver = new BundleNodeRetrieverFactory().build(metadataBuilder)
    val blastDatabase = databaseFactory.build(lm)
    val blast = new BlastFactory().build(lm)
    val giMapper = new InMemoryGIMapperFactory().build(lm)
    val assigner = new Assigner(nodeRetreiver, blastDatabase, giMapper, assignmentConfiguration, extractHeader)
    BlastContext(nodeRetreiver, blastDatabase, blast, assigner)
  }



  def apply(input: List[MergedSampleChunk], s3logger: S3Logger, context: BlastContext): (Map[String, AssignTable], Map[String, ReadsStats]) = {

    import context._

    //todo fix this head
    val chunk = input.head

    //parsing
    val reader = S3ChunksReader(aws.s3, chunk.fastq)
    val parsed: List[FASTQ[RawHeader]] = reader.parseChunk[RawHeader](chunk.range._1, chunk.range._2)._1



    val inputFile = new File("reads.fasta")
    val outputFile = new File("out.blast")
    logger.info("saving reads to " + inputFile.getPath)
    val writer = new PrintWriter(inputFile)
    var emptyInput = true
    parsed.foreach { fastq =>
      val s = fastq.toFasta
      if(emptyInput && !s.trim.isEmpty) {
        emptyInput = false
      }
      writer.println(s)
    }
    writer.close()


    val startTime = System.currentTimeMillis()
    val code = if(emptyInput) {
      logger.warn("empty chunk.. skipping mapping")
      val pw = new PrintWriter(outputFile)
      pw.println("")
      pw.close()
      0
    } else {
      context.blast.launch(blastCommandTemplate, context.database, inputFile, outputFile, useXML)
    }
    val endTime = System.currentTimeMillis()
    logger.info("blast: " + (endTime - startTime + 0.0) / parsed.size + " ms per read")

    if(code != 0) {
      //todo to do something with error message
      throw new Error("BLAST finished with error code " + code)
    }

    if(logging) {
      s3logger.uploadFile(outputFile)
    }


    logger.info("reading BLAST result")
    //todo add xml parser
    val resultRaw = if (useXML) "" else Utils.readFile(outputFile)

    logger.info("parsing BLAST result")

    //M00476_38_000000000_A3FHW_1_1101_20604_2554_1_N_0_28	gi|313494140|gb|GU939576.1|	99.21	253	2	0	1	253	362	614	3e-127	 457

    //last
    //1027    gi|130750839|gb|EF434347.1|     497     253     +       1354    M00476:38:000000000-A3FHW:1:1101:15679:1771     0       253     +       253     253
    val blastHit = """\s*([^\s]+)\s+([^\s]+)\s+([^\s]+).+""".r
    val comment = """#(.*)""".r


    val hits = new ListBuffer[Hit]()
    resultRaw.linesIterator.foreach {
      case comment(c) => //logger.info("skipping comment: " + c)
      case blastHit(header, refId, _score) => {
        val readId = extractHeader(header)
        val score = Utils.parseInt(_score)
        hits += Hit(readId, refId, score)
      }
      case l => logger.error("can't parse: " + l)
    }

    assigner.assign(chunk, parsed, hits.toList)


    //result.toList
  }

}
