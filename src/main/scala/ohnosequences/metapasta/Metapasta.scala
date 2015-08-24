package ohnosequences.metapasta

import ohnosequences.metapasta.instructions._
import ohnosequences.metapasta.reporting._
import ohnosequences.compota._
import ohnosequences.compota.queues.{QueueMerger, ProductQueue}
import ohnosequences.logging.ConsoleLogger
import ohnosequences.awstools.s3.ObjectAddress

import scala.collection.mutable
import scala.util.Try

import java.io.File

abstract class Metapasta(configuration: MetapastaConfiguration) extends Compota {

  override val aws = new AWS(new File(System.getProperty("user.home"), "metapasta.credentials"))

  object compotaConfiguration extends CompotaConfiguration(
    managerGroupConfiguration = configuration.managerGroupConfiguration,
    metamanagerGroupConfiguration = configuration.metamanagerGroupConfiguration,
    defaultInstanceSpecs = configuration.defaultInstanceSpecs,
    metadataBuilder = configuration.metadataBuilder,
    email = configuration.email,
    autoTermination = true,
    timeout = configuration.timeout,
    password = configuration.password,
    removeAllQueues = configuration.removeAllQueues
  ) {
    override def bucket: String = configuration.resultsBucket
  }

  object pairedSamples extends DynamoDBQueue (
    name = "pairedSamples",
    monoid = new ListMonoid[PairedSample],
    serializer = new JsonSerializer[List[PairedSample]],
    throughputs = (1, 1)
  )

  val writeThroughput = configuration.mergeQueueThroughput match {
    case Fixed(m) => m
    case SampleBased(ratio, max) => math.max(ratio * configuration.samples.size, max).toInt
  }

  object mergedSampleChunks extends DynamoDBQueue(
    name = "mergedSampleChunks",
    monoid = new ListMonoid[MergedSampleChunk](),
    serializer = new JsonSerializer[List[MergedSampleChunk]](),
    throughputs = (writeThroughput, 1)
  )

  object readsStats extends S3Queue(
    name = "readsStats",
    monoid = new MapMonoid[(String, AssignmentType), ReadsStats](readsStatsMonoid),
    serializer = readsStatsSerializer
  )

  object assignTable extends S3Queue(
    name = "table",
    monoid = assignTableMonoid,
    serializer = assignTableSerializer
  )

  override val mergingQueues = List(assignTable, readsStats)

  val mergingInstructions: Instructions[List[PairedSample], (List[MergedSampleChunk], Map[(String, AssignmentType), ReadsStats])] =
    new MergingInstructions(configuration, configuration)
  val mergingNispero = nispero(
    inputQueue = pairedSamples,
    outputQueue = ProductQueue(mergedSampleChunks, readsStats),
    instructions = mergingInstructions,
    nisperoConfiguration = NisperoConfiguration(compotaConfiguration, "merge")
  )


  val mappingInstructions: MapInstructions[List[MergedSampleChunk],  (AssignTable, Map[(String, AssignmentType), ReadsStats])] =
    new MappingInstructions(configuration)

  val mapNispero = nispero(
    inputQueue = mergedSampleChunks,
    outputQueue = ProductQueue(assignTable, readsStats),
    instructions = mappingInstructions,
    nisperoConfiguration = NisperoConfiguration(compotaConfiguration, "map", workerGroup = configuration.mappingWorkers)
  )


  override def undeployActions(force: Boolean): Option[String] = {
    if (force) {
      return None
    }

    val logger = new ConsoleLogger("undeploying actions")

    Try {
      logger.info("creating working directory")
      configuration.workingDirectory.mkdir()

      val loadingManager = aws.s3.createLoadingManager()

      logger.info("installing taxonomy")
      configuration.taxonomy.get(logger, configuration.workingDirectory, loadingManager).map { taxonomy =>
        val tableAddress = QueueMerger.destination(compotaConfiguration.results, assignTable)
        val statsAddress = QueueMerger.destination(compotaConfiguration.results,  readsStats)

        logger.info("reading assign table " + tableAddress)

        val tables = assignTable.serializer.fromString(aws.s3.readWholeObject(tableAddress))

        val tagging  = new mutable.HashMap[SampleId, List[SampleTag]]()

        for ((sample, tags) <- configuration.tagging) {
          tagging.put(SampleId(sample.name), tags)
        }

        val reporter = new Reporter(aws, List(tableAddress), List(statsAddress), tagging.toMap, taxonomy,
          ObjectAddress(compotaConfiguration.bucket, "results"), compotaConfiguration.id)
        reporter.generate()


        logger.info("merge FASTA files")

        val reads = ObjectAddress(compotaConfiguration.bucket, "reads")
        val results = ObjectAddress(compotaConfiguration.bucket, "results")

        val merger = new FastaMerger(aws, reads, configuration, configuration.samples.map(_.id))
        merger.merge()


      }
    }

    //todo
    None


  }



  def additionalHandler(args: List[String]) {

    args match {
      case "merge" :: "fastas" :: Nil => {
        val reads = ObjectAddress(compotaConfiguration.bucket, "reads")
        val results = ObjectAddress(compotaConfiguration.bucket, "results")

        val merger = new FastaMerger(aws, reads, configuration, configuration.samples.map(_.id))
        merger.merge()
      }
      case "undeploy" :: "actions" :: Nil => undeployActions(false)
      case _ =>  logger.error("wrong command")
    }
  }


  override def checkConfiguration(verbose: Boolean): Boolean = {
    logger.info("checking samples")
    configuration.samples.forall { sample =>
      val t = aws.s3.objectExists(sample.fastq1, None)
      if (verbose) println("aws.s3.objectExists(" + sample.fastq1 + ") = " + t)
      t
    } &&
     configuration.samples.forall { sample =>
      val t = aws.s3.objectExists(sample.fastq2, None)
      if (verbose) println("aws.s3.objectExists(" + sample.fastq2 + ") = " + t)
      t
    } && {
      logger.info("checking tagging")
      configuration.tagging.forall { case (sample, tags) =>
        configuration.samples.contains(sample)
      }
    } && super.checkConfiguration(verbose)
  }

  def addTasks() {
      pairedSamples.initWrite()
      val t1 = System.currentTimeMillis()
      configuration.samples.foreach {
        sample =>
          pairedSamples.put(sample.name, "", List(List(sample)))
      }
      val t2 = System.currentTimeMillis()
      logger.info("tasks added (in " + (t2 - t1) + " ms)")
  }
}
