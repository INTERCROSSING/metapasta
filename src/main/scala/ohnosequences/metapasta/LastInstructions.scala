package ohnosequences.metapasta

import ohnosequences.nisperon.{Monoid, MapInstructions, AWS}
import ohnosequences.awstools.s3.ObjectAddress
import java.io.{PrintWriter, File}
import org.clapper.avsl.Logger
import scala.collection.mutable.ListBuffer
import ohnosequences.parsers.S3ChunksReader
import ohnosequences.formats.{RawHeader, FASTQ}
import scala.collection.mutable

//ed message
//[2014/01/16 05:01:04:509 UTC] (WARN) Nisperon$DynamoDBQueueLocal: skipping expir
//ed message
//[2014/01/16 05:01:04:543 UTC] (WARN) Nisperon$DynamoDBQueueLocal: skipping expir
//ed message
////[2014/01/16 05:01:12:441 UTC] (INFO) Worker: message read in 9918
//todo ask for contains!!!
case class BestHit(map: Map[String, String])

object BestHitMonoid extends Monoid[BestHit] {
  def unit: BestHit = BestHit(Map[String, String]())

  //todo think about map concatination!!!
  def mult(x: BestHit, y: BestHit): BestHit = BestHit(x.map ++ y.map)
}
//warning: invalid idStatus Code: 400, AWS Service: AmazonSQS, AWS Request ID: a40
//f7ebc-9d9d-54bd-80b9-35ee84f811a7, AWS Error Code: InvalidParameterValue, AWS Er
//ror Message: Value gH2qdC6bjNvmP5H/juIWKzYoWL9V7m+uHwgv+ycAU6XqxaR5xWXU+YkfnMUhw
//6cYdJYopXXLEQn3zvLYK7h+x24hkgFR9NcrkzUx3eO/IRaZjsuraWk/VyzNxMlO2wNsKvaEcOOZMAYtH
//X9zxTRUbLUqyd86L+8E89itA0q3ybDDjAnHrd0ZjcpYtaxMBez6esYlMoyo/ZjQt+QCTBnp8WqHHxcvs
//jK79JKl9Jyb5R9YPbKCLM4Lcs8JIpbsY7Ntgv1UPaxNv+vgegQP8Y/O0/mZSgGjVXhXW3B7Pai+dqE=
//for parameter ReceiptHandle is invalid. Reason: Message does not exist or is not
//available for visibility timeout change.

//todo message read in 1.5 sec with a lot of expired
//todo mesages in the flight 1
class LastInstructions(aws: AWS, database: Database) extends MapInstructions[List[MergedSampleChunk], BestHit] {

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

  def extractHeader(s: String) = s.replace("@", "").replace(" ", "_")

  def apply(input: List[MergedSampleChunk]): BestHit = {



    import scala.sys.process._

    val chunk = input.head

    //parsing
    val reader = S3ChunksReader(aws.s3, chunk.fastq)
    val parsed: List[FASTQ[RawHeader]] = reader.parseChunk[RawHeader](chunk.range._1, chunk.range._2)._1


    logger.info("saving reads to reads.fastq")
    val writer = new PrintWriter(new File("reads.fastq"))
    parsed.foreach { fastq =>
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
    val result = ListBuffer[BlastResult]()
    //todo reads without hits!!!
    //M00476_38_000000000_A3FHW_1_1101_20604_2554_1_N_0_28	gi|313494140|gb|GU939576.1|	99.21	253	2	0	1	253	362	614	3e-127	 457

    //last
    //1027    gi|130750839|gb|EF434347.1|     497     253     +       1354    M00476:38:000000000-A3FHW:1:1101:15679:1771     0       253     +       253     253
    val blastHit = """\s*([^\s]+)\s+([^\s]+)\s+([^\s]+).+""".r
    val lastHit = """\s*([^\s]+)\s+([^\s]+)\s+([^\s]+)\s+([^\s]+)\s+([^\s]+)\s+([^\s]+)\s+([^\s]+)\s+.+""".r
    val comment = """#(.*)""".r

    val bestHits = mutable.HashMap[String, String]()

    parsed.foreach { fastq =>
      bestHits.put(extractHeader(fastq.header.toString), "")
    }
    resultRaw.linesIterator.foreach {
      case comment(c) => logger.info("skipping comment: " + c)
      case lastHit(score, name1, start1, algSize1, strand1, seqSize1, name2) =>
        try {
          val readId = extractHeader(name2)
          //result += BlastResult(name2, database.parseGI(name1))
          if (!bestHits.contains(readId)) {
            bestHits.put(readId, database.parseGI(name1))
          }
        } catch {
          case t: Throwable => t.printStackTrace()
        }
      case l => logger.error("can't parse: " + l)
    }
    BestHit(bestHits.toMap)
    //result.toList
  }

}
