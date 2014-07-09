package ohnosequences.metapasta

import ohnosequences.nisperon._
import ohnosequences.nisperon.bundles.NisperonMetadataBuilder
import ohnosequences.awstools.ec2.{InstanceSpecs, InstanceType}
import ohnosequences.awstools.autoscaling.OnDemand
import ohnosequences.nisperon.queues.{Merger, unitQueue, ProductQueue}
import com.amazonaws.services.dynamodbv2.model._
import java.io.{PrintWriter, File}
import scala.collection.mutable
import ohnosequences.nisperon.Group
import ohnosequences.nisperon.NisperonConfiguration
import ohnosequences.nisperon.NisperoConfiguration
import ohnosequences.awstools.s3.ObjectAddress
import ohnosequences.metapasta.instructions.{LastInstructions, BlastInstructions, FlashInstructions, DynamoDBUploader}
import ohnosequences.metapasta.reporting.{TaxonomyRank, SampleId, FileTypeA, CSVGenerator}


abstract class Metapasta(configuration: MetapastaConfiguration) extends Nisperon {

  val nisperonConfiguration: NisperonConfiguration = NisperonConfiguration(
    managerGroupConfiguration = configuration.managerGroupConfiguration,
    metamanagerGroupConfiguration = configuration.metamanagerGroupConfiguration,
    defaultInstanceSpecs = configuration.defaultInstanceSpecs,
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
    monoid = new MapMonoid[(String, String), ReadsStats](readsStatsMonoid),
    serializer = new JsonSerializer[Map[(String, String), ReadsStats]]
  )


  val assignTable = s3queue(
    name = "table",
    monoid = assignTableMonoid,
    serializer = new JsonSerializer[AssignTable]
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


  val mappingInstructions: MapInstructions[List[MergedSampleChunk],  (AssignTable, Map[(String, String), ReadsStats])] =
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

    val tableAddress = Merger.mergeDestination(Metapasta.this, assignTable)

    logger.info("reading assign table " + tableAddress)

    val tables = assignTable.serializer.fromString(aws.s3.readWholeObject(tableAddress))
    val samples: List[SampleId] = configuration.samples.map { s => SampleId(s.name)}

    val ranks: List[Option[TaxonomyRank]] = TaxonomyRank.ranks.map(Some(_)) ++ List(None)

    for (r <- ranks) {

      val name = r match {
        case None => "A.frequencies.csv"
        case Some(rr) => "A.$rank$.frequencies.csv".replace("$rank$", rr.toString)
      }

//      val genA = new FileTypeA(
//        aws = aws,
//        destination = ObjectAddress(nisperonConfiguration.bucket, "results") / name,
//        nodeRetriever = nodeRetriever,
//        assignments = tables,
//        samples = samples,
//        rank = r
//      )
//
//      genA.generateCSV()
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
    undeployActions(false)
  }


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
