package ohnosequences.metapasta

import ohnosequences.nisperon._
import ohnosequences.nisperon.bundles.NisperonMetadataBuilder
import ohnosequences.nisperon.NisperonConfiguration
import ohnosequences.awstools.s3.ObjectAddress
import ohnosequences.awstools.ec2.InstanceType
import ohnosequences.awstools.autoscaling.OnDemand
import ohnosequences.nisperon.queues.{unitQueue, ProductQueue}
import com.amazonaws.services.dynamodbv2.model.{AttributeValue, Condition, QueryRequest, ScanRequest, ComparisonOperator}


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

  val readsInfo = s3queue(
    name = "readsInfo",
    monoid = new ListMonoid[ReadInfo],
    serializer = new JsonSerializer[List[ReadInfo]]
  )

  val assignTable = s3queue(
    name = "table",
    monoid = AssignTableMonoid,
    serializer = new JsonSerializer[AssignTable]
  )

  override val mergingQueues = List(assignTable)

  //todo think about buffered writing!!

  //todo bucket thing!!!
  val flashNispero = nispero(
    inputQueue = pairedSamples,
    outputQueue = mergedSampleChunks,
    instructions = new FlashInstructions(aws, nisperonConfiguration.bucket),
    nisperoConfiguration = NisperoConfiguration(nisperonConfiguration, "flashNispero")
  )

  val lastNispero = nispero(
    inputQueue = mergedSampleChunks,
    outputQueue = ProductQueue(readsInfo, assignTable),
    instructions = new LastInstructions(aws, new NTLastDatabase(aws)),
    nisperoConfiguration = NisperoConfiguration(nisperonConfiguration, "last", workerGroup = Group(size = 2, max = 15, instanceType = InstanceType.M1Medium, purchaseModel = OnDemand))
  )

  val uploaderNispero = nispero(
    inputQueue = readsInfo,
    outputQueue = unitQueue,
    instructions = new DynamoDBUploader(aws, nisperonConfiguration.id + "_reads", nisperonConfiguration.id + "_chunks"),
    nisperoConfiguration = NisperoConfiguration(nisperonConfiguration, "uploader", workerGroup = Group(size = 1, max = 15, instanceType = InstanceType.T1Micro))
  )


  def undeployActions() {

    println("undeploy!")
  }

  def checks() {
    val sample = "test"
    import scala.collection.JavaConversions._


    val chunks: List[String] = aws.ddb.query(new QueryRequest()
      .withTableName(nisperonConfiguration.id + "_chunks")
      .withKeyConditions(Map("sample" ->
      new Condition()
        .withAttributeValueList(new AttributeValue().withS(sample))
        .withComparisonOperator(ComparisonOperator.EQ)
       ))
    ).getItems.map(_.get("chunk").getS).toList

    var a = 0
    var b = 0
    for(chunk <- chunks) {
      var stopped = false
      while(!stopped) {
        try{
          val reads = aws.ddb.query(new QueryRequest()
            .withTableName(nisperonConfiguration.id + "_reads")
            .withAttributesToGet("header", "gi")
            .withKeyConditions(Map("chunk" ->
            new Condition()
            .withAttributeValueList(new AttributeValue().withS(chunk))
            .withComparisonOperator(ComparisonOperator.EQ)
          ))
          ).getItems.map(_.get("gi").getS).toList

           val n =  reads.filter(_.equals("118136038")).size
          val t  = reads.size



          a += n
          b += t
          println("n: " + n)
          stopped = true
        } catch {
          case t: Throwable => Thread.sleep(1000); println("retry")
        }
      }
    }

    println("unassigned:  " + a)
    println("total:  " + b)
  }

  def addTasks() {
  //  val bio4j = new Bio4jDistributionDist(blastNispero.managerDistribution.metadata)
  //  val noderetr = bio4j.nodeRetriever

    //noderetr.


   // val list = aws.s3.listObjects("releases.era7.com", "ohnosequences")
   // println(list.size)

  //  val set = list.toSet
  //  println(set.size)



    pairedSamples.initWrite()

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
