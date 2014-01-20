package ohnosequences.metapasta

import ohnosequences.nisperon.{MapInstructions, AWS}
import com.amazonaws.services.dynamodbv2.model._
import scala.collection.JavaConversions._
import org.clapper.avsl.Logger
import ohnosequences.awstools.ddb.Utils


class DynamoDBUploader(aws: AWS, table: String) extends MapInstructions[List[ReadInfo], Unit] {

  val logger = Logger(this.getClass)

  def prepare() {
    Utils.createTable(aws.ddb, table, ReadInfo.hash, Some(ReadInfo.range), logger, 100, 100)
  }

  def apply(input: List[ReadInfo]) {
    input.grouped(25).foreach { chunk =>
      val writeOperations = new java.util.ArrayList[WriteRequest]()
      chunk.foreach { readInfo =>
        writeOperations.add(new WriteRequest()
          .withPutRequest(new PutRequest()
          .withItem(readInfo.toDynamoItem)
          ))
      }
      if (!writeOperations.isEmpty) {

        var operations: java.util.Map[String, java.util.List[WriteRequest]] = new java.util.HashMap[String, java.util.List[WriteRequest]]()
        operations.put(table, writeOperations)
        do {
          //to
          try {
            val res = aws.ddb.batchWriteItem(new BatchWriteItemRequest()
              .withRequestItems(operations)            )
            operations = res.getUnprocessedItems
            val size = operations.values().map(_.size()).sum
            logger.info("unprocessed: " + size)
          } catch {
            case t: ProvisionedThroughputExceededException => logger.warn(t.toString + " " + t.getMessage)
          }
        } while (!operations.isEmpty)
      }
    }
  }
}
