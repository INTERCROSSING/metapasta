package ohnosequences.metapasta

import ohnosequences.nisperon._
import org.clapper.avsl.Logger
import scala.collection.mutable
import ohnosequences.parsers.S3ChunksReader
import ohnosequences.formats.{RawHeader, FASTQ}
import java.io.{File, PrintWriter}
import ohnosequences.awstools.s3.ObjectAddress
import scala.collection.mutable.ListBuffer


class BlastInstructions(aws: AWS,
                       database: BlastDatabase,
                       bio4j: Bio4jDistributionDist,
                       blastTemplate: String = """blastn -task megablast -db $name$ -query $input$ -out $output$ -max_target_seqs 1 -num_threads 1 -outfmt 6 -show_gis"""
                       ) extends
   MapInstructions[List[MergedSampleChunk], (List[ReadInfo], AssignTable)] with NodeRetriever {

  val logger = Logger(this.getClass)

  val giMap = new mutable.HashMap[String, String]()

  var nodeRetriever: com.ohnosequences.bio4j.titan.model.util.NodeRetrieverTitan = null

  override def prepare() {


    logger.info("installing bio4j")
    println(bio4j.installWithDeps(ohnosequences.bio4j.bundles.NCBITaxonomyDistribution))
    logger.info("getting database connection")
    nodeRetriever = ohnosequences.bio4j.bundles.NCBITaxonomyDistribution.nodeRetriever

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

    logger.info("downloading gi mapping")
    val mappingFile = new File("gi.map")
    lm.download(ObjectAddress("metapasta", "gi.map"), mappingFile)
    val giP = """(\d+)\s+(\d+).*""".r
    for(line <- io.Source.fromFile(mappingFile).getLines()) {
      line match {
        case giP(gi, tax) => giMap.put(gi, tax)
        case l => logger.error("can't parse " + l)
      }
    }

  }


  def getParerntsIds(tax: String): List[String] = {
    val res = mutable.ListBuffer[String]()
    val node = nodeRetriever.getNCBITaxonByTaxId(tax)
    if(node==null) {
      logger.error("can't receive node for " + tax)
    } else {
      var parent = node.getParent()
      while (parent != null) {
        res += parent.getTaxId()
        //println(parent.getTaxId())
        parent = parent.getParent()
      }
    }
   // println(res.size)
    res.toList
  }

  def writeFile(s: String, file: File) {
    val writer = new PrintWriter(file)
    writer.print(s)
    writer.close()
  }

  def readFile(file: File): String = {
    scala.io.Source.fromFile(file).mkString
  }

  //todo think about this space
  def extractHeader(s: String) = s.replace("@", "").split("\\s")(0)

  def apply(input: List[MergedSampleChunk]): (List[ReadInfo], AssignTable) = {



    import scala.sys.process._

    val chunk = input.head

    //parsing
    val reader = S3ChunksReader(aws.s3, chunk.fastq)
    val parsed: List[FASTQ[RawHeader]] = reader.parseChunk[RawHeader](chunk.range._1, chunk.range._2)._1

   // val reads = mutable.HashMap[String, FASTQ[RawHeader]]()


    val readsFile = "reads.fasta"
    logger.info("saving reads to " + readsFile)
    val writer = new PrintWriter(new File(readsFile))
    parsed.foreach { fastq =>
      //reads.put(extractHeader(fastq.header.toString), fastq)
      writer.println(fastq.toFasta)
    }
    writer.close()

    logger.info("running BLAST")
    val output = "out.blast"
    val command =  blastTemplate
      .replace("$name$", database.name)
      .replace("$output$", output)
      .replace("$input$", readsFile)


    val startTime = System.currentTimeMillis()
    val code = command.!
    val endTime = System.currentTimeMillis()

    logger.info("blast: " + (endTime - startTime + 0.0) / parsed.size + " ms per read")

    if(code != 0) {
      throw new Error("BLAST finished with error code " + code)
    }

    logger.info("reading BLAST result")
    val resultRaw = readFile(new File(output))

    logger.info("parsing BLAST result")
    //todo reads without hits!!!
    //M00476_38_000000000_A3FHW_1_1101_20604_2554_1_N_0_28	gi|313494140|gb|GU939576.1|	99.21	253	2	0	1	253	362	614	3e-127	 457

    //last
    //1027    gi|130750839|gb|EF434347.1|     497     253     +       1354    M00476:38:000000000-A3FHW:1:1101:15679:1771     0       253     +       253     253
    val blastHit = """\s*([^\s]+)\s+([^\s]+)\s+([^\s]+).+""".r
  // val lastHit = """\s*([^\s]+)\s+([^\s]+)\s+([^\s]+)\s+([^\s]+)\s+([^\s]+)\s+([^\s]+)\s+([^\s]+)\s+.+""".r
    val comment = """#(.*)""".r

    val bestHits = mutable.HashMap[String, String]()

    val assignTable = mutable.HashMap[String, TaxInfo]()

//    parsed.foreach { fastq =>
//      bestHits.put(extractHeader(fastq.header.toString), "")
//    }

    resultRaw.linesIterator.foreach {
      case comment(c) => //logger.info("skipping comment: " + c)
      case blastHit(header, refId, score) =>
        try {
          val readId = extractHeader(header)
          //result += BlastResult(name2, database.parseGI(name1))

        //  println("put " + readId)
          //todo best hit
          if (!bestHits.contains(readId)) {
            val gi = database.parseGI(refId)
            bestHits.put(readId, gi)

          //  logger.info("receiving node " + gi)

            val tax = giMap.getOrElse(gi, "gi_" + gi)

            assignTable.get(tax) match {
              case None => assignTable.put(tax, TaxInfo(1, 1))
              case Some(TaxInfo(count, acc)) => assignTable.put(tax, TaxInfo(count + 1, acc + 1))
            }

            if(!tax.startsWith("gi_")) {
              getParerntsIds(tax).foreach { p =>
                assignTable.get(p) match {
                  case None => assignTable.put(p, TaxInfo(0, 1))
                  case Some(TaxInfo(count, acc)) => assignTable.put(p, TaxInfo(count, acc + 1))
                }
              }
            }
          }
        } catch {
          case t: Throwable => t.printStackTrace()
        }
      case l => logger.error("can't parse: " + l)
    }

    val readsInfo = new ListBuffer[ReadInfo]()

    var unassigned = 0

    parsed.foreach { fastq =>
      val readId = extractHeader(fastq.header.toString.replaceAll("\\s+", "_"))
     // println("final: " + readId)
      bestHits.get(readId) match {
        case None => {
          readsInfo += ReadInfo(readId, ReadInfo.unassigned, fastq.sequence, fastq.quality, chunk.sample, chunk.chunkId, ReadInfo.unassigned)
          unassigned += 1
        }
        case Some(g) => {
          val tax = giMap.getOrElse(g, "gi_" + g)
          readsInfo += ReadInfo(readId, g, fastq.sequence, fastq.quality, chunk.sample, chunk.chunkId, tax)
        }
      }
    }

    assignTable.put(ReadInfo.unassigned, TaxInfo(unassigned, unassigned))
    readsInfo.toList -> AssignTable(Map(chunk.sample -> assignTable.toMap))
    //result.toList
  }

}
