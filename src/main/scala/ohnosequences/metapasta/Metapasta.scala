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

  //todo think about buffered writing!!

  //todo bucket thing!!!
  val flashNispero = nispero(
    inputQueue = pairedSamples,
    outputQueue = mergedSampleChunks,
    instructions = new FlashInstructions(aws, nisperonConfiguration.bucket),
    nisperoConfiguration = NisperoConfiguration(nisperonConfiguration, "flashNispero")
  )

  val lastInstructions =  new LastInstructions(aws, new NTLastDatabase(aws))
  val lastNispero = nispero(
    inputQueue = mergedSampleChunks,
    outputQueue = ProductQueue(readsInfo, assignTable),
    instructions = lastInstructions,
    nisperoConfiguration = NisperoConfiguration(nisperonConfiguration, "last", workerGroup = Group(size = 1, max = 15, instanceType = InstanceType.M1Large, purchaseModel = OnDemand))
  )

//  val uploaderNispero = nispero(
//    inputQueue = readsInfo,
//    outputQueue = unitQueue,
//    instructions = new DynamoDBUploader(aws, nisperonConfiguration.id + "_reads", nisperonConfiguration.id + "_chunks"),
//    nisperoConfiguration = NisperoConfiguration(nisperonConfiguration, "uploader", workerGroup = Group(size = 1, max = 15, instanceType = InstanceType.T1Micro))
//  )


  def undeployActions(solved: Boolean) {
    if (!solved) {
      return
    }
    lastInstructions.prepare()
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
    resultCSV.append("name;")
    table.keys.foreach { sample =>
      resultCSV.append(sample + ".count;")
      resultCSV.append(sample + ".acc;")
    }
    resultCSV.append("total.count;total.acc\n")

    var totalCount1 = 0
    var totalAcc1 = 0
    finalTaxInfo.foreach { case (taxid, map) =>
      resultCSV.append(taxid + ";")
      val name = try {
        lastInstructions.nodeRetriver.getNCBITaxonByTaxId(taxid).getScientificName()
      } catch {
        case t: Throwable => ""
      }

      resultCSV.append(name + ";")
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
    resultCSV.append("total; ;")
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

  def ntgi() {
    val ntFile = new File("nt.fasta")
    val s = scala.io.Source.fromFile(ntFile)
    val l = new mutable.HashMap[String, String]()
    val giP = """>gi\|(\d+)\|.+""".r
    logger.info("processing " + ntFile.getPath)
    var counter = 0
    for(line <- s.getLines()) {
      counter += 1
      if(counter % 10000 ==0) {
        logger.info("processed " + counter)
      }
      line match {
        case giP(gi) => l.put(gi, "")
        case ss if ss.startsWith(">") => logger.error(ss)
        case _ =>
      }
    }
    logger.info("finished processing " + ntFile.getPath)
    logger.info("parsed " + l.size + " items")

    val giFile = new File("gi.dmp")
    val taxP = """\s*(\d+)\s+(\d+)\s*""".r
    val taxSource = scala.io.Source.fromFile(giFile)
    logger.info("parsing " + giFile.getPath)
    counter = 0
    val gimap = new PrintWriter(new File("gi.map"))
    val undef = new PrintWriter(new File("gi.undef"))
    for(line <- taxSource.getLines()) {
      counter += 1
      if(counter % 100000 ==0) {
        logger.info("processed " + counter)
      }
      line match {
        case taxP(gi, taxId) => {
          if(l.contains(gi)) {
            l.put(gi, taxId)
            gimap.println(gi + " " + taxId)
          }
        }
        case unparsed => logger.error(unparsed)
      }
    }

    l.toList.foreach { case (gi, tax) =>
      if(tax.isEmpty) {
        undef.println(gi)
      }
    }
    undef.close()
    gimap.close()






    logger.info("finished processing " + giFile.getPath)
//    counter = 0
//    l.toList.grouped(25).foreach { chunk =>
//      counter += 1
//      logger.info("chunk " + counter)
//      val writeOperations = new java.util.ArrayList[WriteRequest]()
//      chunk.foreach { case (gi, tax) =>
//        val item = new util.HashMap[String, AttributeValue]()
//        item.put("gi" , new AttributeValue().withS(gi))
//        item.put("tax" , new AttributeValue().withS(tax))
//
//        if (!tax.isEmpty) {
//        writeOperations.add(new WriteRequest()
//          .withPutRequest(new PutRequest()
//          .withItem(item)
//          ))
//        } else {
//          logger.error(gi)
//        }
//      }
//
//      if (!writeOperations.isEmpty) {
//
//        var operations: java.util.Map[String, java.util.List[WriteRequest]] = new java.util.HashMap[String, java.util.List[WriteRequest]]()
//        operations.put("nt_gi_index", writeOperations)
//        do {
//          //to
//          try {
//            val res = aws.ddb.batchWriteItem(new BatchWriteItemRequest()
//              .withRequestItems(operations)            )
//            operations = res.getUnprocessedItems
//           // val size = operations.values().map(_.size()).sum
//           // logger.info("unprocessed: " + size)
//          } catch {
//            case t: ProvisionedThroughputExceededException => logger.warn(t.toString + " " + t.getMessage)
//          }
//        } while (!operations.isEmpty)
//      }
//    }


   // println(l.take(10).toList)

  }

  def additionalHandler(args: List[String]) {
    undeployActions(true)
    //ntgi()
//    logger.info("additional" + args)
//    args match {
//    case "bio4j" :: Nil => {
//      import ohnosequences.bio4j.distributions._
//      logger.info("installing bio4j")
//      println(Bio4jDistributionDist2.installWithDeps(Bio4jDistribution.GITaxonomyIndex))
//      logger.info("getting database connection")
//      val nodeRetriver =Bio4jDistribution.GITaxonomyIndex.nodeRetriever
//      val gi = "23953857"
//                  logger.info("receiving node " + gi)
//                  val node = nodeRetriver.getNCBITaxonByGiId(gi)
//
//                  if (node != null) {
//                    println("name: " + node.getName())
//                    println("taxid: " + node.getTaxId())
//                    println("rank: " + node.getRank())
//
//                    val parent = node.getParent()
//                    println("name: " + parent.getName())
//                    println("taxid: " + parent.getTaxId())
//                    println("rank: " + parent.getRank())
//                  } else {
//                    logger.info("received null")
//                  }
//    }
//  }

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

    val ss1 = "SRR172902"
    val s1 = PairedSample(ss1, ObjectAddress(testBucket, "mock/" + ss1 + ".fastq"), ObjectAddress(testBucket, "mock/" + ss1 + ".fastq"))

    val ss2 = "SRR172903"
    val s2 = PairedSample(ss2, ObjectAddress(testBucket, "mock/" + ss2 + ".fastq"), ObjectAddress(testBucket, "mock/" + ss2 + ".fastq"))
  //  val sample = PairedSample("test", ObjectAddress(testBucket, "test1.fastq"), ObjectAddress(testBucket, "test2.fastq"))

    pairedSamples.put("0", List(List(s1), List(s2)))

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
