package ohnosequences.metapasta

import ohnosequences.nisperon._
import ohnosequences.nisperon.bundles.NisperonMetadataBuilder
import ohnosequences.awstools.ec2.InstanceType
import ohnosequences.awstools.autoscaling.OnDemand
import ohnosequences.nisperon.queues.{unitQueue}
import com.amazonaws.services.dynamodbv2.model._
import java.io.{PrintWriter, File}
import scala.collection.mutable
import ohnosequences.nisperon.Group
import ohnosequences.nisperon.NisperonConfiguration
import ohnosequences.nisperon.NisperoConfiguration
import ohnosequences.awstools.s3.ObjectAddress
import ohnosequences.nisperon.queues.ProductQueue
import ohnosequences.metapasta.instructions.{LastInstructions, BlastInstructions, FlashInstructions, DynamoDBUploader}
import ohnosequences.metapasta.reporting.CSVGenerator


abstract class Metapasta(configuration: MetapastaConfiguration) extends Nisperon {

  val nisperonConfiguration: NisperonConfiguration = NisperonConfiguration(
    metadataBuilder = configuration.metadataBuilder,
    email = configuration.email,
    autoTermination = true,
    timeout = configuration.timeout,
    password = configuration.password,
    keyName = configuration.keyName,
    removeAllQueues = configuration.removeAllQueues
  )

  val pairedSamples = queue(
    name = "pairedSamples",
    monoid = new ListMonoid[PairedSample],
    serializer = new JsonSerializer[List[PairedSample]],
    throughputs = (1, 1)
  )

  val writeThrouput = configuration.mergeQueueThroughput match {
    case Fixed(m) => m
    case SampleBased(ratio, max) => math.max(ratio * configuration.samples.size, max).toInt
  }

  val mergedSampleChunks = queue(
    name = "mergedSampleChunks",
    monoid = new ListMonoid[MergedSampleChunk](),
    serializer = new JsonSerializer[List[MergedSampleChunk]](),
    throughputs = (writeThrouput, 1)
  )

  val readsStats = s3queue(
    name = "readsStats",
    monoid = new MapMonoid[String, ReadsStats](readsStatsMonoid),
    serializer = new JsonSerializer[Map[String, ReadsStats]]
  )


  val assignTable = s3queue(
    name = "table",
    monoid = new MapMonoid[String, AssignTable](assignTableMonoid),
    serializer = new JsonSerializer[Map[String, AssignTable]]
  )

  override val mergingQueues = List(assignTable, readsStats)

  val flashNispero = nispero(
    inputQueue = pairedSamples,
    outputQueue = ProductQueue(readsStats, mergedSampleChunks),
    instructions = new FlashInstructions(aws, nisperonConfiguration.bucket, configuration.chunksSize),
    nisperoConfiguration = NisperoConfiguration(nisperonConfiguration, "flash")
  )

  val bio4j = new Bio4jDistributionDist(configuration.metadataBuilder)

  //val lastInstructions =  new LastInstructions(aws, new NTLastDatabase(aws), bio4j, configuration.lastTemplate)


  val mappingInstructions: MapInstructions[List[MergedSampleChunk],  (Map[String, AssignTable], Map[String, ReadsStats])] =
    configuration match {
      case b: BlastConfiguration => new BlastInstructions(
        aws = aws,
        metadataBuilder = configuration.metadataBuilder,
        assignmentConfiguration = b.assignmentConfiguration,
        blastCommandTemplate = b.blastTemplate,
        databaseFactory = b.databaseFactory,
        useXML = b.xmlOutput,
        logging = configuration.logging
      )
      case l: LastConfiguration => new LastInstructions(
        aws = aws,
        metadataBuilder = configuration.metadataBuilder,
        assignmentConfiguration = l.assignmentConfiguration,
        lastCommandTemplate = l.lastTemplate,
        databaseFactory = l.databaseFactory,
        fastaInput = l.useFasta,
        logging = configuration.logging
      )
    }


  val mapNispero = nispero(
    inputQueue = mergedSampleChunks,
    outputQueue = ProductQueue(assignTable, readsStats),
    instructions = mappingInstructions,
    nisperoConfiguration = NisperoConfiguration(nisperonConfiguration, "map", workerGroup = configuration.mappingWorkers)
  )

//  configuration.uploadWorkers match {
//    case Some(workers) =>
//      val uploaderNispero = nispero(
//        inputQueue = readsInfo,
//        outputQueue = unitQueue,
//        instructions = new DynamoDBUploader(aws, nisperonConfiguration.id + "_reads", nisperonConfiguration.id + "_chunks"),
//        nisperoConfiguration = NisperoConfiguration(nisperonConfiguration, "upload", workerGroup = Group(size = workers, max = 15, instanceType = InstanceType.T1Micro))
//      )
//    case None => ()
//  }


  //todo test failed actions ...
  override def undeployActions(force: Boolean): Option[String] = {
    if (force) {
      return None
    }

    val nodeRetriever = new BundleNodeRetrieverFactory().build(configuration.metadataBuilder)

    //create csv
    val csvGeneartor = new CSVGenerator(this, nodeRetriever)

    //???
    None



  }

  def checks() {
//    val sample = "test"
//    import scala.collection.JavaConversions._
//
//
//    val chunks: List[String] = aws.ddb.query(new QueryRequest()
//      .withTableName(nisperonConfiguration.id + "_chunks")
//      .withKeyConditions(Map("sample" ->
//      new Condition()
//        .withAttributeValueList(new AttributeValue().withS(sample))
//        .withComparisonOperator(ComparisonOperator.EQ)
//    ))
//    ).getItems.map(_.get("chunk").getS).toList
//
//    var a = 0
//    var b = 0
//    for (chunk <- chunks) {
//      var stopped = false
//      while (!stopped) {
//        try {
//          val reads = aws.ddb.query(new QueryRequest()
//            .withTableName(nisperonConfiguration.id + "_reads")
//            .withAttributesToGet("header", "gi")
//            .withKeyConditions(Map("chunk" ->
//            new Condition()
//              .withAttributeValueList(new AttributeValue().withS(chunk))
//              .withComparisonOperator(ComparisonOperator.EQ)
//          ))
//          ).getItems.map(_.get("gi").getS).toList
//
//          val n = reads.filter(_.equals("118136038")).size
//          val t = reads.size
//
//
//          a += n
//          b += t
//          println("n: " + n)
//          stopped = true
//        } catch {
//          case t: Throwable => Thread.sleep(1000); println("retry")
//        }
//      }
//    }
//
//    println("unassigned:  " + a)
//    println("total:  " + b)
  }

  def additionalHandler(args: List[String]) {
    undeployActions(true)
  }


  //  def additionalHandler(args: List[String]) {
  //    val file = ObjectAddress("metapasta-test", "microtest.fastq")
  //    val chunks = new S3Splitter(aws.s3, file, 10000).chunks()
  //    import ohnosequences.formats._
  //    import ohnosequences.parsers._
  //    val reader = S3ChunksReader(aws.s3, file)
  //    var left = chunks.size
  //    var size = 0
  //    for (chunk <- chunks) {
  //      println(left +  " chunks left")
  //      left -= 1
  //      val parsed: List[FASTQ[RawHeader]] = reader.parseChunk[RawHeader](chunk._1, chunk._2)._1
  //      size += parsed.size
  //    }
  //    println(size)
  //  }


  def checkTasks(): Boolean = {
    var res = true
    logger.info("checking samples")
    configuration.samples.foreach {
      sample =>

        try {
        //  println("aws.s3.objectExists(sample.fastq1)")
          aws.s3.objectExists(sample.fastq1)
        } catch {
          case t: Throwable => {
            res = false
            logger.error("check sample " + sample.fastq1)
            t.printStackTrace()
          }
        }

        try {
          aws.s3.objectExists(sample.fastq2)
        } catch {
          case t: Throwable => {
            res = false
            logger.error("check sample " + sample.fastq2)
            t.printStackTrace()
          }
        }
    }
    res
  }

  def addTasks() {
    if (checkTasks()) {
      pairedSamples.initWrite()
      val t1 = System.currentTimeMillis()
      configuration.samples.foreach {
        sample =>
          pairedSamples.put(sample.name, "", List(List(sample)))
      }
      val t2 = System.currentTimeMillis()
      logger.info("added " + (t2 - t1) + " ms")
    }
  }
}
