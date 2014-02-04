package ohnosequences.metapasta

import ohnosequences.nisperon.{Monoid, MapInstructions, AWS}
import ohnosequences.awstools.s3.ObjectAddress
import java.io.{PrintWriter, File}
import org.clapper.avsl.Logger
import scala.collection.mutable.ListBuffer
import ohnosequences.parsers.S3ChunksReader
import ohnosequences.formats.{RawHeader, FASTQ}
import scala.collection.mutable
import com.amazonaws.services.dynamodbv2.model.{ScalarAttributeType, AttributeDefinition, AttributeValue}



//todo rank, name ...
case class TaxInfo(count: Int, acc: Int)

object TaxInfoMonoid extends Monoid[TaxInfo] {
  def unit: TaxInfo = TaxInfo(0, 0)

  def mult(x: TaxInfo, y: TaxInfo): TaxInfo = TaxInfo(x.count + y.count, x.acc + y.acc)
}

//sample -> (tax -> taxinfo)
case class AssignTable(table: Map[String, Map[String, TaxInfo]])


object AssignTableMonoid extends Monoid[AssignTable] {
  def unit: AssignTable = AssignTable(Map[String, Map[String, TaxInfo]]())

  def mult(x: AssignTable, y: AssignTable): AssignTable = {
    val preRes = mutable.HashMap[String, Map[String, TaxInfo]]()

    for (sample <- x.table.keySet ++ y.table.keySet) {

      val prepreRes = mutable.HashMap[String, TaxInfo]()

      x.table.getOrElse(sample,  Map[String, TaxInfo]()).foreach { case (tax, taxInfo) =>
        prepreRes.get(tax) match {
          case None => prepreRes.put(tax, taxInfo)
          case Some(taxInfo2) => prepreRes.put(tax, TaxInfoMonoid.mult(taxInfo, taxInfo2))
        }
        ///prepreRes.put(gi, n)
      }

      y.table.getOrElse(sample, Map[String, TaxInfo]()).foreach { case (tax, taxInfo) =>
        prepreRes.get(tax) match {
          case None => prepreRes.put(tax, taxInfo)
          case Some(taxInfo2) => prepreRes.put(tax, TaxInfoMonoid.mult(taxInfo, taxInfo2))
        }
      }
      preRes.put(sample, prepreRes.toMap)
    }
    AssignTable(preRes.toMap)

  }
}

case class ReadInfo(readId: String, gi: String, sequence: String, quality: String, sample: String, chunk: String, tax: String) {

  import ReadInfo._

  def chunkId(c: Int) = c + "-" + chunk

  def toDynamoItem(c: Int): java.util.Map[String, AttributeValue] = {
    val r = new java.util.HashMap[String, AttributeValue]()

      r.put(idAttr, new AttributeValue().withS(readId))
      r.put(sequenceAttr, new AttributeValue().withS(sequence))
      r.put(qualityAttr, new AttributeValue().withS(quality))
      r.put(chunkAttr, new AttributeValue().withS(chunkId(c)))
    if(!gi.isEmpty) {
      r.put(giAttr, new AttributeValue().withS(gi))
    } else {
      r.put(giAttr, new AttributeValue().withS(unassigned))
    }

    if(!tax.isEmpty) {
      r.put(taxAttr, new AttributeValue().withS(tax))
    } else {
      r.put(taxAttr, new AttributeValue().withS(unassigned))
    }
      r

  }
}

object ReadInfo {
  val unassigned = "unassigned"

  val idAttr = "header"
  val sequenceAttr = "seq"
  val qualityAttr = "qual"
  val giAttr = "gi"
  val taxAttr = "tax"
  val chunkAttr = "chunk"

  val hash = new AttributeDefinition().withAttributeName(chunkAttr).withAttributeType(ScalarAttributeType.S)
  val range = new AttributeDefinition().withAttributeName(idAttr).withAttributeType(ScalarAttributeType.S)

}



class LastInstructions(aws: AWS, database: Database, bio4j: Bio4jDistributionDist,
                       lastTemplate: String
                       ) extends
   MapInstructions[List[MergedSampleChunk], (List[ReadInfo], AssignTable)] {

  val logger = Logger(this.getClass)

  val giMap = new mutable.HashMap[String, String]()

  var nodeRetriver: com.ohnosequences.bio4j.titan.model.util.NodeRetrieverTitan = null

  override def prepare() {


    logger.info("installing bio4j")
    println(bio4j.installWithDeps(ohnosequences.bio4j.bundles.NCBITaxonomyDistribution))
    logger.info("getting database connection")
    nodeRetriver = ohnosequences.bio4j.bundles.NCBITaxonomyDistribution.nodeRetriever

    logger.info("installing database")
    database.install()

    logger.info("downloading LAST")
    val last = ObjectAddress("metapasta", "lastal")
    val lm = aws.s3.createLoadingManager()

    val f = new File("lastal")
    lm.download(last, f)
    f.setExecutable(true)
    // Runtime.getRuntime.exec("""cp ./ncbi-blast-2.2.25+/bin/* /usr/bin""").exitValue()

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
    val node = nodeRetriver.getNCBITaxonByTaxId(tax)
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

    val reads = mutable.HashMap[String, FASTQ[RawHeader]]()


    logger.info("saving reads to reads.fastq")
    val writer = new PrintWriter(new File("reads.fastq"))
    parsed.foreach { fastq =>
      reads.put(extractHeader(fastq.header.toString), fastq)
      writer.println(fastq.toFastq)
    }
    writer.close()

    logger.info("running LAST")
    val command =  lastTemplate.replace("$name$", database.name)
   // val command = """blastn -task megablast -db $name$ -query reads.fasta -out result -max_target_seqs 1 -num_threads 1 -outfmt 6 -show_gis"""
   //   .replace("$name$", database.name)

    val startTime = System.currentTimeMillis()
    val code = command.!
    val endTime = System.currentTimeMillis()

    logger.info("last: " + (endTime - startTime + 0.0) / parsed.size + " ms per read")

    if(code != 0) {
      throw new Error("LAST finished with error code " + code)
    }

    logger.info("reading BLAST result")
    val resultRaw = readFile(new File("out.last.maf"))

    logger.info("parsing BLAST result")
    //todo reads without hits!!!
    //M00476_38_000000000_A3FHW_1_1101_20604_2554_1_N_0_28	gi|313494140|gb|GU939576.1|	99.21	253	2	0	1	253	362	614	3e-127	 457

    //last
    //1027    gi|130750839|gb|EF434347.1|     497     253     +       1354    M00476:38:000000000-A3FHW:1:1101:15679:1771     0       253     +       253     253
   // val blastHit = """\s*([^\s]+)\s+([^\s]+)\s+([^\s]+).+""".r
    val lastHit = """\s*([^\s]+)\s+([^\s]+)\s+([^\s]+)\s+([^\s]+)\s+([^\s]+)\s+([^\s]+)\s+([^\s]+)\s+.+""".r
    val comment = """#(.*)""".r

    val bestHits = mutable.HashMap[String, String]()

    val assignTable = mutable.HashMap[String, TaxInfo]()

//    parsed.foreach { fastq =>
//      bestHits.put(extractHeader(fastq.header.toString), "")
//    }

    resultRaw.linesIterator.foreach {
      case comment(c) => //logger.info("skipping comment: " + c)
      case lastHit(score, name1, start1, algSize1, strand1, seqSize1, name2) =>
        try {
          val readId = extractHeader(name2)
          //result += BlastResult(name2, database.parseGI(name1))

          //todo best hit
          if (!bestHits.contains(readId)) {
            bestHits.put(readId, database.parseGI(name1))

            val gi = database.parseGI(name1)
          //  logger.info("receiving node " + gi)

            val tax = giMap.getOrElse(gi, "gi_" + gi)


           // val node = nodeRetriver.getNCBITaxonByTaxId(gi)

            //todo parent


//            if (node != null) {
//              println("name: " + node.getName())
//              println("taxid: " + node.getTaxId())
//              println("rank: " + node.getRank())
//
//              val parent = node.getParent()
//              println("name: " + parent.getName())
//              println("taxid: " + parent.getTaxId())
//              println("rank: " + parent.getRank())
//            } else {
//              logger.info("received null")
//            }

            assignTable.get(tax) match {
              case None => assignTable.put(tax, TaxInfo(1, 1))
              case Some(TaxInfo(count, acc)) => assignTable.put(tax, TaxInfo(count + 1, acc + 1))
            }


            //adding parents
            if(!tax.startsWith("gi_")) {
              getParerntsIds(tax).foreach { p =>
                assignTable.get(p) match {
                  case None => assignTable.put(p, TaxInfo(0, 1))
                  case Some(TaxInfo(count, acc)) => assignTable.put(p, TaxInfo(count, acc + 1))
                }
              }
             }


           // println(neandertal)
          }
        } catch {
          case t: Throwable => t.printStackTrace()
        }
      case l => logger.error("can't parse: " + l)
    }

    val readsInfo = new ListBuffer[ReadInfo]()

    var unassigned = 0

    parsed.foreach { fastq =>
      val readId = extractHeader(fastq.header.toString)
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
