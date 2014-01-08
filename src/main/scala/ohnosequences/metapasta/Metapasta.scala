package ohnosequences.metapasta

import ohnosequences.nisperon._
import ohnosequences.nisperon.bundles.NisperonMetadataBuilder
import ohnosequences.nisperon.NisperonConfiguration
import ohnosequences.awstools.s3.ObjectAddress


object testInstructions extends MapInstructions[Int, Int] {
  def apply(input: Int): Int = input * input
}

object Metapasta extends Nisperon {
  val nisperonConfiguration: NisperonConfiguration = NisperonConfiguration(
    metadataBuilder = new NisperonMetadataBuilder(new generated.metadata.metapasta()),
    email = "museeer@gmail.com",
    autoTermination = false,
    timeout = 36000
  )

  val pairedSample = queue(
    name = "pairedSample",
    monoid = new ListMonoid[PairedSample],
    serializer = new JsonSerializer[List[PairedSample]]
  )

  val processedSample = queue(
    name = "processedSample",
    monoid = new ListMonoid[ProcessedSampleChunk](),
    serializer = new JsonSerializer[List[ProcessedSampleChunk]]()
  )

  val parsedSample = queue(
    name = "parsedSample",
    monoid = new ListMonoid[ParsedSampleChunk](),
    serializer = new JsonSerializer[List[ParsedSampleChunk]](),
    writeBodyToTable = false
  )

  val blastRes = s3queue(
    name = "blastRes",
    monoid = new ListMonoid[BlastResult],
    serializer = new JsonSerializer[List[BlastResult]]
  )

  override val mergingQueues = List(blastRes)

  //todo think about buffered writing!!

  //todo bucket thing!!!
  val flashNispero = nispero(
    inputQueue = pairedSample,
    outputQueue = processedSample,
    instructions = new FlashInstructions(aws, nisperonConfiguration.id.replace("_", "-").toLowerCase),
    nisperoConfiguration = NisperoConfiguration(nisperonConfiguration, "flashNispero")
  )

  val parseNispero = nispero(
    inputQueue = processedSample,
    outputQueue = parsedSample,
    instructions = new ParseInstructions(aws),
    nisperoConfiguration = NisperoConfiguration(nisperonConfiguration, "parse")
  )


  val blastNispero = nispero(
    inputQueue = parsedSample,
    outputQueue = blastRes,
    instructions = new BlastInstructions(aws, new NTDatabase(aws)),
    nisperoConfiguration = NisperoConfiguration(nisperonConfiguration, "blast")
  )


  def undeployActions() {
    println("undeploy!")
  }

  def addTasks() {
  //  val bio4j = new Bio4jDistributionDist(blastNispero.managerDistribution.metadata)
  //  val noderetr = bio4j.nodeRetriever

    //noderetr.


    pairedSample.init()

    val t1 = System.currentTimeMillis()

//    for (i <- 1 to n) {
//      if(i % 100 == 0) {
//        println((n - i) + " left")
//
//      }
//      queue1.put("id" + i, List(i))
//    }

    val testBucket = "metapasta-test"
    val sample = PairedSample("test", ObjectAddress(testBucket, "test1.fastq"), ObjectAddress(testBucket, "test2.fastq"))

    pairedSample.put("000", List(List(sample)))

    //todo fix this order!!!
    //added 232 ms
    //check you e-mail for further instructions
    //  unprocessed:0
   // pairedSample.put("0", List(io.Source.fromFile("f1.fasta").mkString, io.Source.fromFile("f2.fasta").mkString))


    val t2 = System.currentTimeMillis()

    println("added " + (t2-t1) + " ms")

    //should be initialized

   // checkQueues()

   // println(queue1.list())

  }
}
