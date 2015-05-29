package ohnosequences.metapasta

import ohnosequences.benchmark.Bench
import ohnosequences.compota.{AnyNispero, AnyCompota}
import ohnosequences.compota.aws.queues.{DynamoDBContext, DynamoDBQueue}
import ohnosequences.compota.aws._
import ohnosequences.compota.environment.AnyEnvironment
import ohnosequences.compota.monoid.{Monoid, ListMonoid}
import ohnosequences.compota.queues.{AnyQueue, ProductQueue, Queue, AnyQueueReducer}
import ohnosequences.compota.serialization.JsonSerializer
import ohnosequences.metapasta.instructions.{MergingInstructions, MappingInstructions}

import scala.collection.mutable
import ohnosequences.awstools.s3.ObjectAddress
import ohnosequences.metapasta.reporting._
import java.io.File


trait AnyMetapasta extends AnyCompota {

 // val metapastaConfiguration: MetapastaConfiguration


  override type CompotaConfiguration <: MetapastaConfiguration

  type MetapastaEnvironment <: AnyEnvironment[MetapastaEnvironment]
  type QueueContext

  //E, Ctx, M <: AnyQueueMessage.of[E], R <: AnyQueueReader.of[E, M], W <: AnyQueueWriter.of[E], O <: AnyQueueOp.of[E, M, R, W]
 // type M1 <:
  type PairedSamplesQueue <: AnyQueue.of2[List[PairedSample], QueueContext]
  val pairedSamplesQueue: PairedSamplesQueue
  val pairedSampleMonoid = new ListMonoid[PairedSample]()

  type MergedSamplesQueue <: Queue[List[MergedSampleChunk], QueueContext]
  val mergedSampleChunksQueue: MergedSamplesQueue
  val mergedSamplesMonoid = new ListMonoid[MergedSampleChunk]

  type ReadsStatsQueue <: Queue[Map[(String, AssignmentType), ReadsStats], QueueContext]
  val readsStatsQueue: ReadsStatsQueue
  val readsStatsMonoid2: Monoid[Map[(String, AssignmentType), ReadsStats]] = readStatMapMonoid

  type AssignTableQueue <: Queue[AssignTable, QueueContext]
  val assignTableQueue: AssignTableQueue

  //class ProductQueue[Ctx, X, Y,
//XM <: AnyQueueMessage.of[X], XR <: AnyQueueReader.of[X, XM], XW <: AnyQueueWriter.of[X], XO <: AnyQueueOp.of[X, XM, XR, XW], XQ <: AnyQueue.of3[X, Ctx, XM, XR, XW, XO],
//YM <: AnyQueueMessage.of[Y], YR <: AnyQueueReader.of[Y, YM], YW <: AnyQueueWriter.of[Y], YO <: AnyQueueOp.of[Y, YM, YR, YW], YQ <: AnyQueue.of3[Y, Ctx, YM, YR, YW, YO]
//]
  //

  type MerginQueueOutput = AnyQueue.of2[(List[MergedSampleChunk], Map[(String, AssignmentType), ReadsStats]), QueueContext]

  val mergingOutputQueue: MerginQueueOutput = new ProductQueue[QueueContext,
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

  object mergingInstructions extends MergingInstructions(configuration)

  object mappingInstructions extends MappingInstructions(configuration)

  type MergingNispero <: AnyNispero.of3[MetapastaEnvironment,
    List[PairedSample],
    (List[MergedSampleChunk], Map[(String, AssignmentType), ReadsStats]),
    QueueContext,
    PairedSamplesQueue,
    MerginQueueOutput
    ]

  val mergingNispero: MergingNispero

}





abstract class AwsMetapasta(val configuration: AwsMetapastaConfiguration) extends AnyMetapasta with AnyAwsCompota {

  //override val aws = new AWS(new File(System.getProperty("user.home"), "metapasta.credentials"))


  override type CompotaConfiguration = AwsMetapastaConfiguration

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

  override type ReadsStatsQueue = readsStatsQueue2.type


  override val readsStatsQueue: ReadsStatsQueue = readsStatsQueue2

  object readsStatsQueue2 extends DynamoDBQueue(
    name = "readsStats",
    serializer = readsStatsSerializer,
    bench = None,
    readThroughput = configuration.readStatsQueueThroughput.read,
    writeThroughput = configuration.readStatsQueueThroughput.write
  )


  override type AssignTableQueue = assignTableQueue2.type


  override val assignTableQueue: AssignTableQueue = assignTableQueue2

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



  override type MergingNispero = mergingNispero2.type
  override val mergingNispero: MergingNispero = mergingNispero2


  object mergingNispero2 extends AwsNispero[
    List[PairedSample],
    (List[MergedSampleChunk], Map[(String, AssignmentType), ReadsStats]),
    QueueContext,
    QueueContext,
    PairedSamplesQueue,
    MerginQueueOutput
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
