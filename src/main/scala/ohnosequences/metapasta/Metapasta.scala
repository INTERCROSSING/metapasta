package ohnosequences.metapasta

import ohnosequences.benchmark.Bench
import ohnosequences.compota.aws.queues.{DynamoDBContext, DynamoDBQueue}
import ohnosequences.compota.aws.{AwsEnvironment, AnyAwsCompota, AwsNispero, AwsCompota}
import ohnosequences.compota.environment.AnyEnvironment
import ohnosequences.compota.monoid.ListMonoid
import ohnosequences.compota.queues.{Queue, AnyQueueReducer}
import ohnosequences.compota.serialization.JsonSerializer
import ohnosequences.metapasta.instructions.{MergingInstructions, MappingInstructions}

import scala.collection.mutable
import ohnosequences.awstools.s3.ObjectAddress
import ohnosequences.metapasta.reporting._
import java.io.File


trait AnyMetapasta {

  val metapastaConfiguration: MetapastaConfiguration

  type MetapastaEnvironment <: AnyEnvironment[MetapastaEnvironment]
  type QueueContext

  type PairedSamplesQueue <: Queue[List[PairedSample], QueueContext]
  val pairedSamplesQueue: PairedSamplesQueue

  type MergedSamplesQueue <: Queue[List[MergedSampleChunk], QueueContext]
  val mergedSampleChunksQueue: MergedSamplesQueue

  type ReadsStatsQueue <: Queue[Map[(String, AssignmentType), ReadsStats], QueueContext]
  val readsStatsQueue: ReadsStatsQueue

  type AssignTableQueue <: Queue[AssignTable, QueueContext]
  val assignTableQueue: AssignTableQueue

  object mergingInstructions extends MergingInstructions(metapastaConfiguration)

  object mappingInstructions extends MappingInstructions(metapastaConfiguration)

}





abstract class AwsMetapasta(val configuration: AwsMetapastaConfiguration) extends AnyMetapasta with AnyAwsCompota {

  //override val aws = new AWS(new File(System.getProperty("user.home"), "metapasta.credentials"))

  type QueueContext = DynamoDBContext

  object pairedSamplesQueue extends DynamoDBQueue (
    name = "pairedSamples",
    serializer = new JsonSerializer[List[PairedSample]],
    bench = None,
    readThroughput = configuration.pairedSampleQueueThroughput.read,
    writeThroughput = configuration.pairedSampleQueueThroughput.write
  )

//  val writeThroughput = configuration.mergeQueueThroughput match {
//    case Fixed(m) => m
//    case SampleBased(ratio, max) => math.max(ratio * configuration.samples.size, max).toInt
//  }

  object mergedSampleChunks extends DynamoDBQueue(
    name = "mergedSampleChunks",
    serializer = new JsonSerializer[List[MergedSampleChunk]](),
    bench = None,
    readThroughput = configuration.mergedSampleQueueThroughput.read,
    writeThroughput = configuration.mergedSampleQueueThroughput.write
  )

  object readsStats extends DynamoDBQueue(
    name = "readsStats",
    serializer = readsStatsSerializer,
    bench = None,
    readThroughput = configuration.readStatsQueueThroughput.read,
    writeThroughput = configuration.readStatsQueueThroughput.write
  )


  object assignTable extends DynamoDBQueue(
    name = "assignTable",
    serializer = assignTableSerializer,
    bench = None,
    readThroughput = configuration.assignTableQueueThroughput.read,
    writeThroughput = configuration.assignTableQueueThroughput.write
  )


  override val reducers = List[AnyQueueReducer.of[CompotaEnvironment]]()


  object mappingNispero extends AwsNispero(
    inputQueue = mergedSampleChunks, {t: AwsEnvironment => t.createDynamoDBContext()},
    outputQueue = mergedSampleChunks, {t: AwsEnvironment => t.createDynamoDBContext()},

    instructions = mappingInstructions,


  )

  val flashNispero = nispero(
    inputQueue = pairedSamples,
    outputQueue = ProductQueue(readsStats, mergedSampleChunks),
    instructions = new FlashInstructions(
      aws, configuration.chunksSize, ObjectAddress(nisperonConfiguration.bucket, "reads"),
    configuration.chunksThreshold, configuration.flashTemplate),
    nisperoConfiguration = NisperoConfiguration(nisperonConfiguration, "flash")
  )

  val bio4j = new Bio4jDistributionDist(configuration.metadataBuilder)

  //val lastInstructions =  new LastInstructions(aws, new NTLastDatabase(aws), bio4j, configuration.lastTemplate)




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

    val tableAddress = QueueMerger.destination(nisperonConfiguration.results, assignTable)
    val statsAddress = QueueMerger.destination(nisperonConfiguration.results,  readsStats)

    logger.info("reading assign table " + tableAddress)

    val tables = assignTable.serializer.fromString(aws.s3.readWholeObject(tableAddress))

    val tagging  = new mutable.HashMap[SampleId, List[SampleTag]]()

    for ((sample, tags) <- configuration.tagging) {
      tagging.put(SampleId(sample.name), tags)
    }

    val reporter = new Reporter(aws, List(tableAddress), List(statsAddress), tagging.toMap, nodeRetriever,
      ObjectAddress(nisperonConfiguration.bucket, "results"), nisperonConfiguration.id)
    reporter.generate()


    logger.info("merge fastas")

    val reads = ObjectAddress(nisperonConfiguration.bucket, "reads")
    val results = ObjectAddress(nisperonConfiguration.bucket, "results")

    val merger = new FastaMerger(aws, reads, results, configuration.samples.map(_.name))
    merger.merge()

    if(configuration.generateDot) {
      logger.info("generate dot files")
      DOTExporter.installGraphiz()
      tables.table.foreach { case (sampleAssignmentType, map) =>
        val sample = sampleAssignmentType._1
        val assignmentType = sampleAssignmentType._2
        val dotFile = new File(sample  + "." + assignmentType + ".tree.dot")
        val pdfFile = new File(sample  + "." + assignmentType + ".tree.pdf")
        DOTExporter.generateDot(map, nodeRetriever.nodeRetriever,dotFile)
        DOTExporter.generatePdf(dotFile, pdfFile)
        aws.s3.putObject(S3Paths.treeDot(results, sample, assignmentType), pdfFile)
        aws.s3.putObject(S3Paths.treePdf(results, sample, assignmentType), pdfFile)
      }
    }


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

    args match {
      case "merge" :: "fastas" :: Nil => {
        val reads = ObjectAddress(nisperonConfiguration.bucket, "reads")
        val results = ObjectAddress(nisperonConfiguration.bucket, "results")

        val merger = new FastaMerger(aws, reads, results, configuration.samples.map(_.name))
        merger.merge()
      }
      case _ =>  undeployActions(false)
    }
  }


  override def checkTasks(verbose: Boolean): Boolean = {

    logger.info("checking samples")
    configuration.samples.forall { sample =>
      val t = aws.s3.objectExists(sample.fastq1, Some(logger))
      if (verbose) println("aws.s3.objectExists(" + sample.fastq1 + ") = " + t)
      t
    } &&
     configuration.samples.forall { sample =>
      val t = aws.s3.objectExists(sample.fastq2, Some(logger))
      if (verbose) println("aws.s3.objectExists(" + sample.fastq2 + ") = " + t)
      t
    }
  }

  def addTasks() {
    //logger.info("creating bucket " + bucket)
    aws.s3.createBucket(nisperonConfiguration.bucket)

    if (checkTasks(false)) {
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
