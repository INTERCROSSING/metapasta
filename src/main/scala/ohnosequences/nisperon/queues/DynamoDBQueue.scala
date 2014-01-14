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


//think about batch stuff latter
class DynamoDBQueue[T](aws: AWS, name: String, monoid: Monoid[T], val serializer: Serializer[T], writeBodyToTable: Boolean, throughputs: (Int, Int)) extends MonoidQueue[T](name, monoid) {

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

  val sqsQueue = new SQSQueue[T](aws.sqs.sqs, name, serializer)
  
  val visibilityExtender = new VisibilityExtender[T](name)
  
  val sqsWriter = new SQSWriter(aws, sqsQueue.queueURL, monoid, name, serializer)
  val ddbWriter = new DynamoDBWriter(aws, monoid, name, serializer, idAttr, valueAttr, writeBodyToTable)
  

  //todo think about this order!
  def put(taskId: String, values: List[T]) {
    var c = 0
    values.filter(!_.equals(monoid.unit)).map {
      value =>
        c += 1
        val id = taskId + "." + c
        sqsWriter.put(id, value)
        ddbWriter.put(id, value)
    }
    sqsWriter.flush()
    ddbWriter.flush()
  }


  def read(): Message[T] = {

    var message: SQSMessage[T] = null

    var taken = false
    while(!taken) {
      message = sqsQueue.readRAW()
      //println("taked: " + message)
      //check timeout
      try {
        message.changeMessageVisibility(20)
        taken = true
      } catch {
        case t: Throwable => logger.warn("skipping expired message")
      }
    }

    visibilityExtender.addMessage(message)

    new Message[T] {
      val id: String = message.id

      def value(): T = message.value()

      def delete() {
        aws.ddb.deleteItem(new DeleteItemRequest()
          .withTableName(name)
          .withKey(Map(idAttr -> new AttributeValue().withS(id)))
        )

        visibilityExtender.clear()
        message.delete()
      }

      def changeMessageVisibility(secs: Int): Unit = message.changeMessageVisibility(secs)
    }
  }

  def init() {
    sqsQueue.init()

    if (!visibilityExtender.isAlive) {
      try {
        visibilityExtender.start()
      } catch {
        case t: IllegalThreadStateException => ()
      }
    }

    sqsWriter.init()
    ddbWriter.init()

    Utils.createTable(aws.ddb, name, new AttributeDefinition(idAttr, ScalarAttributeType.S), None, logger, throughputs._1, throughputs._2)
  }


  //need for reset state...
  def reset() {
    visibilityExtender.clear()
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

//
// //why init is needed:
//object T {
//  println("initialization")
//}
//
//
//
//object M {
//  def main(args: Array[String]) {
//    val t = Some(T)
//    println("T")
//  }
//}
