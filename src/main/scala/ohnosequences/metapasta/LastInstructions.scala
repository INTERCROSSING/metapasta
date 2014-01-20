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

//import com.ohnosequences.bio4j.titan.model.util.NodeRetrieverTitan
//import ohnosequences.bio4j.distributions.Bio4jDistribution.GITaxonomyIndex



case class AssignTable(table: Map[String, Map[String, Int]])


object AssignTableMonoid extends Monoid[AssignTable] {
  def unit: AssignTable = AssignTable(Map[String, Map[String, Int]]())

  //todo think about map concatination!!!
  def mult(x: AssignTable, y: AssignTable): AssignTable = {
    val preRes = mutable.HashMap[String, Map[String, Int]]()

    for (sample <- x.table.keySet ++ y.table.keySet) {
      val prepreRes = mutable.HashMap[String, Int]()
      x.table.getOrElse(sample, Map[String, Int]()).foreach { case (gi, n) =>
        prepreRes.put(gi, n)
      }

      y.table.getOrElse(sample, Map[String, Int]()).foreach { case (gi, n) =>
        prepreRes.get(gi) match {
          case None => prepreRes.put(gi, n)
          case Some(m) => prepreRes.put(gi, m + n)
        }
      }
      preRes.put(sample, prepreRes.toMap)
    }
    AssignTable(preRes.toMap)

  }
}

//ed message
//[2014/01/16 05:01:04:509 UTC] (WARN) Nisperon$DynamoDBQueueLocal: skipping expir
//ed message
//[2014/01/16 05:01:04:543 UTC] (WARN) Nisperon$DynamoDBQueueLocal: skipping expir
//ed message
////[2014/01/16 05:01:12:441 UTC] (INFO) Worker: message read in 9918
//todo ask for contains!!!
//case class ReadsInfo(reasds: Map[String, String])
//
//object BestHitMonoid extends Monoid[BestHit] {
//  def unit: BestHit = BestHit(Map[String, String]())
//
//  //todo think about map concatination!!!
//  def mult(x: BestHit, y: BestHit): BestHit = BestHit(x.map ++ y.map)
//}
//warning: invalid idStatus Code: 400, AWS Service: AmazonSQS, AWS Request ID: a40
//f7ebc-9d9d-54bd-80b9-35ee84f811a7, AWS Error Code: InvalidParameterValue, AWS Er
//ror Message: Value gH2qdC6bjNvmP5H/juIWKzYoWL9V7m+uHwgv+ycAU6XqxaR5xWXU+YkfnMUhw
//6cYdJYopXXLEQn3zvLYK7h+x24hkgFR9NcrkzUx3eO/IRaZjsuraWk/VyzNxMlO2wNsKvaEcOOZMAYtH
//X9zxTRUbLUqyd86L+8E89itA0q3ybDDjAnHrd0ZjcpYtaxMBez6esYlMoyo/ZjQt+QCTBnp8WqHHxcvs
//jK79JKl9Jyb5R9YPbKCLM4Lcs8JIpbsY7Ntgv1UPaxNv+vgegQP8Y/O0/mZSgGjVXhXW3B7Pai+dqE=
//for parameter ReceiptHandle is invalid. Reason: Message does not exist or is not
//available for visibility timeout change.

case class ReadInfo(readId: String, gi: String, sequence: String, quality: String) {


  import ReadInfo._

  def toDynamoItem: java.util.Map[String, AttributeValue] = {
    val r = new java.util.HashMap[String, AttributeValue]()

      r.put(idAttr, new AttributeValue().withS(readId))
      r.put(sequenceAttr, new AttributeValue().withS(sequence))
      r.put(qualityAttr, new AttributeValue().withS(quality))
    if(!gi.isEmpty) {
      r.put(giAttr, new AttributeValue().withS(gi))
    } else {
      r.put(giAttr, new AttributeValue().withS("unassigned"))
    }
      r

  }
}

object ReadInfo {

  val idAttr = "header"
  val sequenceAttr = "seq"
  val qualityAttr = "qual"
  val giAttr = "gi"

  val hash = new AttributeDefinition().withAttributeName(idAttr).withAttributeType(ScalarAttributeType.S)
  val range = new AttributeDefinition().withAttributeName(giAttr).withAttributeType(ScalarAttributeType.S)

}



//todo message read in 1.5 sec with a lot of expired
//todo mesages in the flight 1
class LastInstructions(aws: AWS, database: Database) extends
   MapInstructions[List[MergedSampleChunk], (List[ReadInfo], AssignTable)] {

  val logger = Logger(this.getClass)


 // var nodeRetriver: NodeRetrieverTitan = null

  override def prepare() {

//    import ohnosequences.bio4j.distributions._
//    logger.info("installing bio4j")
//    println(Bio4jDistributionDist2.installWithDeps(Bio4jDistribution.GITaxonomyIndex))
 //   logger.info("getting database connection")
//    nodeRetriver = GITaxonomyIndex.nodeRetriever

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
    val command =  """./lastal nt.last/$name$ reads.fastq -s 2 -T 1 -f 0 -r5 -q95 -a0 -b95 -e400 -Q2 -o out.last.maf"""
      .replace("$name$", database.name)
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
    val blastHit = """\s*([^\s]+)\s+([^\s]+)\s+([^\s]+).+""".r
    val lastHit = """\s*([^\s]+)\s+([^\s]+)\s+([^\s]+)\s+([^\s]+)\s+([^\s]+)\s+([^\s]+)\s+([^\s]+)\s+.+""".r
    val comment = """#(.*)""".r

    val bestHits = mutable.HashMap[String, String]()

    val assignTable = mutable.HashMap[String, Int]()

//    parsed.foreach { fastq =>
//      bestHits.put(extractHeader(fastq.header.toString), "")
//    }

    resultRaw.linesIterator.foreach {
      case comment(c) => logger.info("skipping comment: " + c)
      case lastHit(score, name1, start1, algSize1, strand1, seqSize1, name2) =>
        try {
          val readId = extractHeader(name2)
          //result += BlastResult(name2, database.parseGI(name1))

          //todo best hit
          if (!bestHits.contains(readId)) {
            bestHits.put(readId, database.parseGI(name1))

            val gi = database.parseGI(name1)
//            logger.info("receiving node " + gi)
//            val node = nodeRetriver.getNCBITaxonByGiId(gi)
//
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

            assignTable.get(gi) match {
              case None => assignTable.put(gi, 1)
              case Some(n) => assignTable.put(gi, n + 1)
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
          readsInfo += ReadInfo(readId, "", fastq.sequence, fastq.quality)
          unassigned += 1
        }
        case Some(g) => readsInfo += ReadInfo(readId, g, fastq.sequence, fastq.quality)
      }
    }
    assignTable.put("unassigned", unassigned)
    readsInfo.toList -> AssignTable(Map(chunk.name -> assignTable.toMap))
    //result.toList
  }

}
