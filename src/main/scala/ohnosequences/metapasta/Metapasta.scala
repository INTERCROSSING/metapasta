package ohnosequences.metapasta

import ohnosequences.benchmark.Bench
import ohnosequences.compota.local._
import ohnosequences.compota.queues.AnyQueueReducer.of
import ohnosequences.compota.{InMemoryQueueReducer, AnyNispero, AnyCompota}
import ohnosequences.compota.aws.queues.{DynamoDBContext, DynamoDBQueue}
import ohnosequences.compota.aws._
import ohnosequences.compota.environment.AnyEnvironment
import ohnosequences.compota.monoid.{Monoid, ListMonoid}
import ohnosequences.compota.queues.{AnyQueue, ProductQueue, Queue, AnyQueueReducer}
import ohnosequences.compota.serialization.JsonSerializer
import ohnosequences.metapasta.instructions.{MergingInstructions, MappingInstructions}
import org.jboss.netty.channel.SucceededChannelFuture

import scala.collection.mutable
import ohnosequences.awstools.s3.ObjectAddress
import ohnosequences.metapasta.reporting._
import java.io.File

import scala.util.{Success, Try}


trait AnyMetapasta extends AnyCompota {

  override type CompotaConfiguration <: MetapastaConfiguration

  type MetapastaEnvironment <: AnyEnvironment[MetapastaEnvironment]
  type QueueContext


  type PairedSamplesQueue <: AnyQueue.of2[List[PairedSample], QueueContext]
  def pairedSamplesQueue: PairedSamplesQueue
  def pairedSampleMonoid = new ListMonoid[PairedSample]()

  type MergedSamplesQueue <: Queue[List[MergedSampleChunk], QueueContext]
  def mergedSampleChunksQueue: MergedSamplesQueue
  def mergedSamplesMonoid = new ListMonoid[MergedSampleChunk]

  def readsStatsMonoid2: Monoid[Map[(String, AssignmentType), ReadsStats]] = readStatMapMonoid


  type MergingQueueOutput = AnyQueue.of2[(List[MergedSampleChunk], Map[(String, AssignmentType), ReadsStats]), QueueContext]
  def mergingOutputQueue: MergingQueueOutput

  type MappingQueueOutput = AnyQueue.of2[(AssignTable, Map[(String, AssignmentType), ReadsStats]), QueueContext]
  def mappingOutputQueue: MappingQueueOutput

  object mergingInstructions extends MergingInstructions(
    configuration.mergingInstructionsConfiguration
  )
  object mappingInstructions extends MappingInstructions(configuration.mappingInstructionsConfiguration)

  def mergingNispero: CompotaNispero with AnyNispero.of3[
      MetapastaEnvironment,
      List[PairedSample],
      (List[MergedSampleChunk], Map[(String, AssignmentType), ReadsStats]),
      QueueContext,
      PairedSamplesQueue,
      MergingQueueOutput
  ]

  def mappingNispero: CompotaNispero with AnyNispero.of3[MetapastaEnvironment,
    List[MergedSampleChunk],
    (AssignTable, Map[(String, AssignmentType), ReadsStats]),
    QueueContext,
    MergedSamplesQueue,
    MappingQueueOutput
    ]

  override val nisperos: List[CompotaNispero] = List(mergingNispero, mappingNispero)


  def assignTableReducer: AnyQueueReducer.of[CompotaEnvironment]

  def readsStatReducer: AnyQueueReducer.of[CompotaEnvironment]

}


abstract class LocalMetapasta(val configuration: LocalMetapastaConfiguration) extends AnyMetapasta with AnyLocalCompota {



  override type CompotaUnDeployActionContext = Option[Bio4j]

  override def prepareUnDeployActions(env: CompotaEnvironment): Try[CompotaUnDeployActionContext] = {
    Success(None)
  }

  override def unDeployActions(force: Boolean, env: CompotaEnvironment, context: CompotaUnDeployActionContext): Try[String] = {
    Success("finished")
  }

//  override def addTasks(environment: CompotaEnvironment): Try[Unit] = {
//    environment.logger.info("adding tasks")
//    Success(())
//  }



  override type CompotaConfiguration = LocalMetapastaConfiguration

  override type CompotaNispero = AnyLocalNispero

  override type QueueContext = LocalContext

  override type MetapastaEnvironment = LocalEnvironment

  type PairedSamplesQueue = pairedSamplesQueue2.type
  override val pairedSamplesQueue = pairedSamplesQueue2
  object pairedSamplesQueue2 extends LocalQueue[List[PairedSample]] (
    name = "pairedSamples"
  )

  override type MergedSamplesQueue = mergedSampleChunksQueue2.type
  override val mergedSampleChunksQueue: MergedSamplesQueue = mergedSampleChunksQueue2
  object mergedSampleChunksQueue2 extends LocalQueue[List[MergedSampleChunk]] (
    name = "mergedSampleChunks"
  )

  object readsStatsQueue extends LocalQueue[Map[(String, AssignmentType), ReadsStats]](
    name = "readsStats"
  )

  object assignTableQueue extends LocalQueue[AssignTable](
    name = "assignTable"
  )

  override val mergingOutputQueue: MergingQueueOutput = new ProductQueue[QueueContext,
      List[MergedSampleChunk],
      Map[(String, AssignmentType), ReadsStats],
      mergedSampleChunksQueue.QueueQueueMessage,
      mergedSampleChunksQueue.QueueQueueReader,
      mergedSampleChunksQueue.QueueQueueWriter,
      mergedSampleChunksQueue.QueueQueueOp,
      mergedSampleChunksQueue.type ,
      readsStatsQueue.QueueQueueMessage,
      readsStatsQueue.QueueQueueReader,
      readsStatsQueue.QueueQueueWriter,
      readsStatsQueue.QueueQueueOp,
      readsStatsQueue.type
      ](mergedSampleChunksQueue, readsStatsQueue, mergedSamplesMonoid, readsStatsMonoid2)


  override val mappingOutputQueue: MappingQueueOutput = new ProductQueue[QueueContext,
      AssignTable,
      Map[(String, AssignmentType), ReadsStats],
      assignTableQueue.QueueQueueMessage,
      assignTableQueue.QueueQueueReader,
      assignTableQueue.QueueQueueWriter,
      assignTableQueue.QueueQueueOp,
      assignTableQueue.type ,
      readsStatsQueue.QueueQueueMessage,
      readsStatsQueue.QueueQueueReader,
      readsStatsQueue.QueueQueueWriter,
      readsStatsQueue.QueueQueueOp,
      readsStatsQueue.type
      ](assignTableQueue, readsStatsQueue, assignTableMonoid, readsStatsMonoid2)

  override val mergingNispero = new LocalNispero[
    List[PairedSample],
    (List[MergedSampleChunk], Map[(String, AssignmentType), ReadsStats]),
    LocalContext,
    LocalContext,
    PairedSamplesQueue,
    MergingQueueOutput
    ](
  inputQueue = pairedSamplesQueue, {t: LocalEnvironment => t.localContext},
  outputQueue = mergingOutputQueue, {t: LocalEnvironment => t.localContext},
  instructions = mergingInstructions,
  LocalNisperoConfiguration(configuration, "merge", configuration.mergers)
  )


  override val mappingNispero = new LocalNispero[
    List[MergedSampleChunk],
    (AssignTable, Map[(String, AssignmentType), ReadsStats]),
    LocalContext,
    LocalContext,
    MergedSamplesQueue,
    MappingQueueOutput
    ](
  inputQueue = mergedSampleChunksQueue, {t: LocalEnvironment => t.localContext},
  outputQueue = mappingOutputQueue, {t: LocalEnvironment => t.localContext},
  instructions = mappingInstructions,
  LocalNisperoConfiguration(configuration, "map", configuration.mappers)
  )

  override def readsStatReducer: of[CompotaEnvironment] = InMemoryQueueReducer.apply(readsStatsQueue, readsStatsMonoid2)

  override def assignTableReducer: of[CompotaEnvironment] = InMemoryQueueReducer.apply(assignTableQueue, assignTableMonoid)

  override val reducers: List[of[CompotaEnvironment]] = List(assignTableReducer, readsStatReducer)

}


abstract class AwsMetapasta(val configuration: AwsMetapastaConfiguration) extends AnyMetapasta with AnyAwsCompota {

  //override val aws = new AWS(new File(System.getProperty("user.home"), "metapasta.credentials"))


  override type CompotaConfiguration = AwsMetapastaConfiguration

  override type CompotaNispero = AnyAwsNispero

  type QueueContext = DynamoDBContext


  override type MetapastaEnvironment = AwsEnvironment

  type PairedSamplesQueue = pairedSamplesQueue2.type

  override val pairedSamplesQueue = pairedSamplesQueue2
  object pairedSamplesQueue2 extends DynamoDBQueue(
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

  override type MergedSamplesQueue = mergedSampleChunksQueue2.type


  override val mergedSampleChunksQueue: MergedSamplesQueue = mergedSampleChunksQueue2

  object mergedSampleChunksQueue2 extends DynamoDBQueue(
    name = "mergedSampleChunks",
    serializer = new JsonSerializer[List[MergedSampleChunk]](),
    bench = None,
    readThroughput = configuration.mergedSampleQueueThroughput.read,
    writeThroughput = configuration.mergedSampleQueueThroughput.write
  )

  type ReadsStatsQueue = readsStatsQueue2.type


  val readsStatsQueue: ReadsStatsQueue = readsStatsQueue2

  object readsStatsQueue2 extends DynamoDBQueue(
    name = "readsStats",
    serializer = readsStatsSerializer,
    bench = None,
    readThroughput = configuration.readStatsQueueThroughput.read,
    writeThroughput = configuration.readStatsQueueThroughput.write
  )


  type AssignTableQueue = assignTableQueue2.type


  val assignTableQueue: AssignTableQueue = assignTableQueue2

  object assignTableQueue2 extends DynamoDBQueue(
    name = "assignTable",
    serializer = assignTableSerializer,
    bench = None,
    readThroughput = configuration.assignTableQueueThroughput.read,
    writeThroughput = configuration.assignTableQueueThroughput.write
  )


  object mergingNisperoConfiguration extends AwsNisperoConfiguration {
    override def name: String = "merge"

    override def compotaConfiguration: AwsCompotaConfiguration = configuration
  }



//  override val mergingNispero: MergingNispero = mergingNispero2
  override object mergingNispero extends AwsNispero[
    List[PairedSample],
    (List[MergedSampleChunk], Map[(String, AssignmentType), ReadsStats]),
    QueueContext,
    QueueContext,
    PairedSamplesQueue,
    MergingQueueOutput
    ](
    inputQueue = pairedSamplesQueue, {t: AwsEnvironment => t.createDynamoDBContext()},
    outputQueue = mergingOutputQueue, {t: AwsEnvironment => t.createDynamoDBContext()},
    instructions = mergingInstructions,
    mergingNisperoConfiguration
  )



}



//
//
//  override val reducers = List[AnyQueueReducer.of[CompotaEnvironment]]()
//
//
//  object mappingNispero extends AwsNispero(
//    inputQueue = mergedSampleChunks, {t: AwsEnvironment => t.createDynamoDBContext()},
//    outputQueue = mergedSampleChunks, {t: AwsEnvironment => t.createDynamoDBContext()},
//
//    instructions = mappingInstructions,
//
//
//  )
//
//  val flashNispero = nispero(
//    inputQueue = pairedSamples,
//    outputQueue = ProductQueue(readsStats, mergedSampleChunks),
//    instructions = new FlashInstructions(
//      aws, configuration.chunksSize, ObjectAddress(nisperonConfiguration.bucket, "reads"),
//    configuration.chunksThreshold, configuration.flashTemplate),
//    nisperoConfiguration = NisperoConfiguration(nisperonConfiguration, "flash")
//  )
//
//  val bio4j = new Bio4jDistributionDist(configuration.metadataBuilder)
//
//  //val lastInstructions =  new LastInstructions(aws, new NTLastDatabase(aws), bio4j, configuration.lastTemplate)
//
//
//
//
////  configuration.uploadWorkers match {
////    case Some(workers) =>
////      val uploaderNispero = nispero(
////        inputQueue = readsInfo,
////        outputQueue = unitQueue,
////        instructions = new DynamoDBUploader(aws, nisperonConfiguration.id + "_reads", nisperonConfiguration.id + "_chunks"),
////        nisperoConfiguration = NisperoConfiguration(nisperonConfiguration, "upload", workerGroup = Group(size = workers, max = 15, instanceType = InstanceType.T1Micro))
////      )
////    case None => ()
////  }
//
//
//  //todo test failed actions ...
//  override def undeployActions(force: Boolean): Option[String] = {
//    if (force) {
//      return None
//    }
//
//    val nodeRetriever = new BundleNodeRetrieverFactory().build(configuration.metadataBuilder)
//
//    val tableAddress = QueueMerger.destination(nisperonConfiguration.results, assignTable)
//    val statsAddress = QueueMerger.destination(nisperonConfiguration.results,  readsStats)
//
//    logger.info("reading assign table " + tableAddress)
//
//    val tables = assignTable.serializer.fromString(aws.s3.readWholeObject(tableAddress))
//
//    val tagging  = new mutable.HashMap[SampleId, List[SampleTag]]()
//
//    for ((sample, tags) <- configuration.tagging) {
//      tagging.put(SampleId(sample.name), tags)
//    }
//
//    val reporter = new Reporter(aws, List(tableAddress), List(statsAddress), tagging.toMap, nodeRetriever,
//      ObjectAddress(nisperonConfiguration.bucket, "results"), nisperonConfiguration.id)
//    reporter.generate()
//
//
//    logger.info("merge fastas")
//
//    val reads = ObjectAddress(nisperonConfiguration.bucket, "reads")
//    val results = ObjectAddress(nisperonConfiguration.bucket, "results")
//
//    val merger = new FastaMerger(aws, reads, results, configuration.samples.map(_.name))
//    merger.merge()
//
//    if(configuration.generateDot) {
//      logger.info("generate dot files")
//      DOTExporter.installGraphiz()
//      tables.table.foreach { case (sampleAssignmentType, map) =>
//        val sample = sampleAssignmentType._1
//        val assignmentType = sampleAssignmentType._2
//        val dotFile = new File(sample  + "." + assignmentType + ".tree.dot")
//        val pdfFile = new File(sample  + "." + assignmentType + ".tree.pdf")
//        DOTExporter.generateDot(map, nodeRetriever.nodeRetriever,dotFile)
//        DOTExporter.generatePdf(dotFile, pdfFile)
//        aws.s3.putObject(S3Paths.treeDot(results, sample, assignmentType), pdfFile)
//        aws.s3.putObject(S3Paths.treePdf(results, sample, assignmentType), pdfFile)
//      }
//    }
//
//
//    None
//  }
//
//  def checks() {
////    val sample = "test"
////    import scala.collection.JavaConversions._
////
////
////    val chunks: List[String] = aws.ddb.query(new QueryRequest()
////      .withTableName(nisperonConfiguration.id + "_chunks")
////      .withKeyConditions(Map("sample" ->
////      new Condition()
////        .withAttributeValueList(new AttributeValue().withS(sample))
////        .withComparisonOperator(ComparisonOperator.EQ)
////    ))
////    ).getItems.map(_.get("chunk").getS).toList
////
////    var a = 0
////    var b = 0
////    for (chunk <- chunks) {
////      var stopped = false
////      while (!stopped) {
////        try {
////          val reads = aws.ddb.query(new QueryRequest()
////            .withTableName(nisperonConfiguration.id + "_reads")
////            .withAttributesToGet("header", "gi")
////            .withKeyConditions(Map("chunk" ->
////            new Condition()
////              .withAttributeValueList(new AttributeValue().withS(chunk))
////              .withComparisonOperator(ComparisonOperator.EQ)
////          ))
////          ).getItems.map(_.get("gi").getS).toList
////
////          val n = reads.filter(_.equals("118136038")).size
////          val t = reads.size
////
////
////          a += n
////          b += t
////          println("n: " + n)
////          stopped = true
////        } catch {
////          case t: Throwable => Thread.sleep(1000); println("retry")
////        }
////      }
////    }
////
////    println("unassigned:  " + a)
////    println("total:  " + b)
//  }
//
//  def additionalHandler(args: List[String]) {
//
//    args match {
//      case "merge" :: "fastas" :: Nil => {
//        val reads = ObjectAddress(nisperonConfiguration.bucket, "reads")
//        val results = ObjectAddress(nisperonConfiguration.bucket, "results")
//
//        val merger = new FastaMerger(aws, reads, results, configuration.samples.map(_.name))
//        merger.merge()
//      }
//      case _ =>  undeployActions(false)
//    }
//  }
//
//
//  override def checkTasks(verbose: Boolean): Boolean = {
//
//    logger.info("checking samples")
//    configuration.samples.forall { sample =>
//      val t = aws.s3.objectExists(sample.fastq1, Some(logger))
//      if (verbose) println("aws.s3.objectExists(" + sample.fastq1 + ") = " + t)
//      t
//    } &&
//     configuration.samples.forall { sample =>
//      val t = aws.s3.objectExists(sample.fastq2, Some(logger))
//      if (verbose) println("aws.s3.objectExists(" + sample.fastq2 + ") = " + t)
//      t
//    }
//  }
//
//  def addTasks() {
//    //logger.info("creating bucket " + bucket)
//    aws.s3.createBucket(nisperonConfiguration.bucket)
//
//    if (checkTasks(false)) {
//      pairedSamples.initWrite()
//      val t1 = System.currentTimeMillis()
//      configuration.samples.foreach {
//        sample =>
//          pairedSamples.put(sample.name, "", List(List(sample)))
//      }
//      val t2 = System.currentTimeMillis()
//      logger.info("added " + (t2 - t1) + " ms")
//    }
//  }
//}
