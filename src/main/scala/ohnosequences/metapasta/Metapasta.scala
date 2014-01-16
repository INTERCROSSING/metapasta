package ohnosequences.metapasta

import ohnosequences.nisperon._
import ohnosequences.nisperon.bundles.NisperonMetadataBuilder
import ohnosequences.nisperon.NisperonConfiguration
import ohnosequences.awstools.s3.ObjectAddress
import ohnosequences.awstools.ec2.InstanceType
import ohnosequences.awstools.autoscaling.OnDemand


object testInstructions extends MapInstructions[Int, Int] {
  def apply(input: Int): Int = input * input
}

object Metapasta extends Nisperon {
  val nisperonConfiguration: NisperonConfiguration = NisperonConfiguration(
    metadataBuilder = new NisperonMetadataBuilder(new generated.metadata.metapasta()),
    email = "museeer@gmail.com",
    autoTermination = true,
    timeout = 36000
  )

  val pairedSamples = queue(
    name = "pairedSamples",
    monoid = new ListMonoid[PairedSample],
    serializer = new JsonSerializer[List[PairedSample]],
    throughputs = (1, 1)
  )

  val mergedSampleChunks = queue(
    name = "mergedSampleChunks",
    monoid = new ListMonoid[MergedSampleChunk](),
    serializer = new JsonSerializer[List[MergedSampleChunk]](),
    throughputs = (1, 1)
  )

  val blastRes = s3queue(
    name = "lastRes",
    monoid = BestHitMonoid,
    serializer = new JsonSerializer[BestHit]
  )

  override val mergingQueues = List(blastRes)

  //todo think about buffered writing!!

  //todo bucket thing!!!
  val flashNispero = nispero(
    inputQueue = pairedSamples,
    outputQueue = mergedSampleChunks,
    instructions = new FlashInstructions(aws, nisperonConfiguration.bucket),
    nisperoConfiguration = NisperoConfiguration(nisperonConfiguration, "flashNispero")
  )

  val blastNispero = nispero(
    inputQueue = mergedSampleChunks,
    outputQueue = blastRes,
    instructions = new LastInstructions(aws, new NTLastDatabase(aws)),
    nisperoConfiguration = NisperoConfiguration(nisperonConfiguration, "blast", workerGroup = Group(size = 10, max = 15, instanceType = InstanceType.M1Medium, purchaseModel = OnDemand))
  )


  def undeployActions() {
    println("undeploy!")
  }

  def addTasks() {
  //  val bio4j = new Bio4jDistributionDist(blastNispero.managerDistribution.metadata)
  //  val noderetr = bio4j.nodeRetriever

    //noderetr.


   // val list = aws.s3.listObjects("releases.era7.com", "ohnosequences")
   // println(list.size)

  //  val set = list.toSet
  //  println(set.size)



    pairedSamples.init()

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

    pairedSamples.put("000", List(List(sample)))

    //todo fix this order!!!
    //added 232 ms
    //check you e-mail for further instructions
    //  unprocessed:0
   // pairedSample.put("0", List(io.Source.fromFile("f1.fasta").mkString, io.Source.fromFile("f2.fasta").mkString))


    val t2 = System.currentTimeMillis()

    logger.info("added " + (t2-t1) + " ms")

    //should be initialized

   // checkQueues()

   // println(queue1.list())

  }
}
