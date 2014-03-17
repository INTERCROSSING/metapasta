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


abstract class Metapasta(configuration: MetapastaConfiguration) extends Nisperon {

  val nisperonConfiguration: NisperonConfiguration = NisperonConfiguration(
    metadataBuilder = configuration.metadataBuilder,
    email = configuration.email,
    autoTermination = true,
    timeout = configuration.timeout,
    logging = configuration.logging,
    keyName = configuration.keyName,
    removeAllQueues = configuration.removeAllQueues
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
    throughputs = (5, 1)
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

  val flashNispero = nispero(
    inputQueue = pairedSamples,
    outputQueue = mergedSampleChunks,
    instructions = new FlashInstructions(aws, nisperonConfiguration.bucket, configuration.chunksSize),
    nisperoConfiguration = NisperoConfiguration(nisperonConfiguration, "flash")
  )

  val bio4j = new Bio4jDistributionDist(configuration.metadataBuilder)

  //val lastInstructions =  new LastInstructions(aws, new NTLastDatabase(aws), bio4j, configuration.lastTemplate)


  val mappingInstructions: MapInstructions[List[MergedSampleChunk], (List[ReadInfo], AssignTable)] with NodeRetriever =
    configuration match {
      case b: BlastConfiguration => new BlastInstructions(aws, b.database, bio4j, b.blastTemplate, b.xmlOutput)
      case l: LastConfiguration => new LastInstructions(aws, l.database, bio4j, l.lastTemplate, l.useFasta)
    }


  val lastNispero = nispero(
    inputQueue = mergedSampleChunks,
    outputQueue = ProductQueue(readsInfo, assignTable),
    instructions = mappingInstructions,
    nisperoConfiguration = NisperoConfiguration(nisperonConfiguration, "map", workerGroup = configuration.mappingWorkers)
  )

  configuration.uploadWorkers match {
    case Some(workers) =>
      val uploaderNispero = nispero(
        inputQueue = readsInfo,
        outputQueue = unitQueue,
        instructions = new DynamoDBUploader(aws, nisperonConfiguration.id + "_reads", nisperonConfiguration.id + "_chunks"),
        nisperoConfiguration = NisperoConfiguration(nisperonConfiguration, "upload", workerGroup = Group(size = workers, max = 15, instanceType = InstanceType.T1Micro))
      )
    case None => ()
  }


  override def undeployActions(solved: Boolean): Option[String] = {
    if (!solved) {
      return None
    }

    //todo write generic code about it
    mergedSampleChunks.delete()
    mappingInstructions.prepare()
    //todo think about order
    //create csv
    val resultTableJSON = ObjectAddress(nisperonConfiguration.bucket, "results/" + assignTable.name)

    logger.info("reading assign table " + resultTableJSON)
    val table = assignTable.serializer.fromString(aws.s3.readWholeObject(resultTableJSON)).table

    //tax -> (sample -> taxinfo)
    val resultCSV = new mutable.StringBuilder()
    logger.info("transposing table")
    val finalTaxInfo = mutable.HashMap[String, mutable.HashMap[String, TaxInfo]]()

    val perSampleTotal = mutable.HashMap[String, TaxInfo]()
    var totalCount0 = 0
    var totalAcc0 = 0
    table.foreach { case (sample, map) =>
      var sampleTotal = TaxInfoMonoid.unit

      map.foreach { case (tax, taxInfo) =>
        sampleTotal = TaxInfoMonoid.mult(taxInfo, sampleTotal)
        finalTaxInfo.get(tax) match {
          case None => {
            val initMap = mutable.HashMap[String, TaxInfo](sample -> taxInfo)
            finalTaxInfo.put(tax, initMap)
          }
          case Some(sampleMap) => {
            sampleMap.get(sample) match {
              case None => sampleMap.put(sample, taxInfo)
              case Some(oldTaxInfo) => {
                sampleMap.put(sample, TaxInfoMonoid.mult(taxInfo, oldTaxInfo))
              }
            }
          }
        }
        perSampleTotal.put(sample, sampleTotal)
      }
      totalCount0 += sampleTotal.count
      totalAcc0 += sampleTotal.acc
    }
    
    
    resultCSV.append("#;taxId;")
    resultCSV.append("name;rank;")
    table.keys.foreach { sample =>
      resultCSV.append(sample + ".count;")
      resultCSV.append(sample + ".acc;")
    }
    resultCSV.append("total.count;total.acc\n")

    var totalCount1 = 0
    var totalAcc1 = 0
    finalTaxInfo.foreach { case (taxid, map) =>
      resultCSV.append(taxid + ";")
      val (name, rank) = try {
        val node = mappingInstructions.nodeRetriever.getNCBITaxonByTaxId(taxid)
        (node.getScientificName(), node.getRank())
      } catch {
        case t: Throwable => ("", "")
      }
      resultCSV.append(name + ";")
      resultCSV.append(rank + ";")


      //mappingInstructions.nodeRetriever.g

      var taxCount = 0
      var taxAcc = 0
      table.keys.foreach { sample =>
        map.get(sample) match {
          case Some(taxInfo) => {
            resultCSV.append(taxInfo.count + ";" + taxInfo.acc + ";")
            taxCount += taxInfo.count
            taxAcc += taxInfo.acc
          }
          case None => {
            resultCSV.append(0 + ";" + 0 + ";")
          }
        }
      }
      resultCSV.append(taxCount + ";" + taxAcc + "\n")
      totalCount1 += taxCount
      totalAcc1 += taxAcc
      //calculating total
    }
    resultCSV.append("total; ; ;")
    table.keys.foreach { sample =>
      resultCSV.append(perSampleTotal(sample).count + ";")
      resultCSV.append(perSampleTotal(sample).acc + ";")
    }

    if(totalCount0 == totalCount1) {
      resultCSV.append(totalCount0 + ";")
    } else {
      resultCSV.append("\n# " + totalCount0 + "!=" + totalCount1 + ";")
    }

    if(totalAcc0 == totalAcc1) {
      resultCSV.append(totalAcc0 + "\n")
    } else {
      resultCSV.append("\n# " + totalAcc0 + "!=" + totalAcc1 + "\n")
    }


    val result = ObjectAddress(nisperonConfiguration.bucket, "results/" + "result.csv")
    aws.s3.putWholeObject(result, resultCSV.toString())

    None

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

  def additionalHandler(args: List[String]) {}


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

  def objectExists(objectAddress: ObjectAddress): Boolean = {
   // try { 
      // import com.amazonaws.services.s3.model.GetObjectRequest
      // val request = new GetObjectRequest(objectAddress.bucket, objectAddress.key).withRange(0, 1)
      // val res = aws.s3.s3.getObject(request)
      // true
      if(aws.s3.listObjects(objectAddress.bucket, objectAddress.key).contains(objectAddress)) {
        true
      } else {
        throw new Error(objectAddress + " doesn't exist")
        false
      }
    // } catch {
    //   case t: Throwable=> logger.error("check sample " + objectAddress)          
    //                       t.printStackTrace()
    // }
  }

  def checkTasks(): Boolean = {
    var res = true
    logger.info("checking samples")
    configuration.samples.foreach { sample =>
      
      try {
        objectExists(sample.fastq1)
      } catch {
        case t: Throwable => {
          res = false
          logger.error("check sample " + sample.fastq1)          
          t.printStackTrace()
        }
      }

       try {
        objectExists(sample.fastq2)
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
    if(checkTasks()) {
      pairedSamples.initWrite()
      val t1 = System.currentTimeMillis()
      configuration.samples.foreach { sample =>
        pairedSamples.put(sample.name, List(List(sample)))
      }
      val t2 = System.currentTimeMillis()
      logger.info("added " + (t2-t1) + " ms")
    }
  }
}
