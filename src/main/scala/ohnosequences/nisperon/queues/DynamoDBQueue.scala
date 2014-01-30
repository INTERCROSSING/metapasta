package ohnosequences.nisperon.queues

import ohnosequences.nisperon.{Serializer, Monoid, AWS}
import com.amazonaws.services.dynamodbv2.model._
import scala.collection.JavaConversions._
import org.clapper.avsl.Logger
import ohnosequences.awstools.ddb.Utils
import scala.collection.mutable.ListBuffer

//
//trait Trackable {
//  def resultsIds: Set[String]
//
//}

//todo remove body from tables

//think about batch stuff latter
class DynamoDBQueue[T](
                        aws: AWS,
                        name: String,
                        monoid: Monoid[T],
                        serializer: Serializer[T],
                        throughputs: (Int, Int),
                        deadLetterQueueName: String
                        ) extends MonoidQueue[T](name, monoid, serializer) {

  def createBatchWriteItemRequest(table: String, items: List[Map[String, AttributeValue]]): BatchWriteItemRequest = {
    val writeOperations = new java.util.ArrayList[WriteRequest]()
    items.foreach { item =>
      writeOperations.add(new WriteRequest()
        .withPutRequest(new PutRequest()
        .withItem(item)
        ))
    }

    val map = new java.util.HashMap[String, java.util.List[WriteRequest]]()
    map.put(table, writeOperations)

    new BatchWriteItemRequest().withRequestItems(map)
  }


  val idAttr = "id"
  val valueAttr = "val"

  val logger = Logger(this.getClass)

  val sqsQueue = new SQSQueue[T](aws.sqs.sqs, name, serializer, deadLetterQueueName = Some(deadLetterQueueName))

  var sqsWriter: Option[SQSWriter[T]] = None
  var sqsReader: Option[SQSReader[T]] = None


  val ddbWriter = new DynamoDBWriter(aws, monoid, name, serializer, idAttr, valueAttr, true)
  


  def put(taskId: String, values: List[T]) {
    sqsWriter match {
      case Some(writer) => {
        var c = 0
        values.filter(!_.equals(monoid.unit)).map {
          value =>
            c += 1
            val id = taskId + "." + c
            writer.write(id, value)
            ddbWriter.put(id, value)
        }
        ddbWriter.flush()
        writer.flush()
      }
      case None => throw new Error("write to a not initialized queue")
    }
  }


  def read(): Message[T] = {
    sqsReader match {
      case Some(reader) => {
        val rawMessage = reader.read

        new Message[T] {
          val id: String = rawMessage.id

          def value(): T = rawMessage.value()

          def delete() {
            aws.ddb.deleteItem(new DeleteItemRequest()
              .withTableName(name)
              .withKey(Map(idAttr -> new AttributeValue().withS(id)))
            )
            rawMessage.delete()

          }

          def changeMessageVisibility(secs: Int): Unit = rawMessage.changeMessageVisibility(secs)
        }
      }
      case None => throw new Error("read from a not initialized queue")
    }
  }

  def initRead() {
    init()
    sqsReader = Some(sqsQueue.getReader(false))
  }

  def init() {
    Utils.createTable(aws.ddb, name, new AttributeDefinition(idAttr, ScalarAttributeType.S), None, logger, throughputs._1, throughputs._2)
  }

  def initWrite() {
    init()
    sqsWriter = Some(sqsQueue.getWriter(monoid))
    ddbWriter.init()
  }

  //need for reset state...
  def reset() {
    sqsReader match {
      case Some(reader) => reader.reset()
      case None => throw new Error("reset a not initialized queue")
    }
  }

  def isEmpty: Boolean = {
    val count = aws.ddb.scan(new ScanRequest()
      .withTableName(name)
      .withSelect(Select.COUNT)
      .withLimit(1)
    ).getCount
    println(count)
    count == 0
  }

  def list(): List[String] = {
    val result = ListBuffer[String]()
    var lastKey: java.util.Map[String, AttributeValue] = null
    do {
      val items = aws.ddb.scan(new ScanRequest()
        .withTableName(name)
        .withAttributesToGet(idAttr)
      )
      result ++= items.getItems.map(_.get(idAttr).getS)
      lastKey = items.getLastEvaluatedKey
    } while (lastKey != null)
    result.toList
  }

  def read(id: String): Option[T] = {
    try {
      aws.ddb.getItem(new GetItemRequest()
        .withTableName(name)
        .withKey(Map(idAttr -> new AttributeValue().withS(id)))
      ).getItem match {
        case null => None
        case item => Some(serializer.fromString(item.get(valueAttr).getS))
      }
    } catch {
      case t: Throwable => logger.warn("message not found: " + id); None
    }
  }

  def delete(id: String) {
    try {
      aws.ddb.deleteItem(new DeleteItemRequest()
        .withTableName(name)
        .withKey(Map(idAttr -> new AttributeValue().withS(id)))
      )
    } catch {
      case t: Throwable => logger.warn("message not found: " + id); None
    }
  }
}
