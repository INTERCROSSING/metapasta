package ohnosequences.nisperon.queues

import ohnosequences.nisperon.{Serializer, Monoid, AWS}
import com.amazonaws.services.dynamodbv2.model.{GetItemRequest, AttributeValue, PutItemRequest, ScanRequest}
import scala.collection.JavaConversions._
import java.util.concurrent.ArrayBlockingQueue
import com.amazonaws.services.sqs.model.ReceiveMessageRequest


trait Trackable {
  def resultsIds: Set[String]
  
}


//think about batch stuff latter
class TrackQueue[T](aws: AWS, name: String, monoid: Monoid[T], val serializer: Serializer[T]) extends MonoidQueue[T](name, monoid) with Trackable {
  val taskIdAttr = "id1"
  val resultIdAttr = "id2"

  val unitId = ""

  val readyItem = Map(
    taskIdAttr -> new AttributeValue().withS("ready"),
    resultIdAttr -> new AttributeValue().withS(unitId)
  )




  val logger = Logger(this.getClass)

  val sqsQueue = new SQSQueue[T](aws.sqs.sqs, name, serializer)


  //todo batch write
  //todo throughput
  def put(taskId: String, values: List[T]) {

    val ids: List[String] = values.filter(!_.equals(monoid.unit)).map { value =>
      sqsQueue.put(value)
    }

    if (ids.isEmpty) {
      aws.ddb.putItem(new PutItemRequest()
        .withTableName(name)
        .withItem(Map(
          taskIdAttr -> new AttributeValue().withS(taskId),
          resultIdAttr -> new AttributeValue().withS(unitId)
      ))
      )
    } else {
      ids.foreach { id =>
        aws.ddb.putItem(new PutItemRequest()
          .withTableName(name)
          .withItem(Map(
          taskIdAttr -> new AttributeValue().withS(taskId),
          resultIdAttr -> new AttributeValue().withS(id)
        ))
        )
      }
    }
  }

  def reset(): Unit = {
    visibilityExtender.clear()
  }

    object visibilityExtender extends Thread("extender_" + name) {

      val messages = new java.util.concurrent.ConcurrentHashMap[String, SQSMessage[T]]()


      def addMessage(m: SQSMessage[T]) {
        messages.put(m.receiptHandle, m)
      }

      def clear() {
        messages.clear()
      }

      override def run() {

        while(true) {
          messages.values().foreach { m =>
            try {
              m.changeMessageVisibility(20)
            } catch {
              case t: Throwable => {
                println("warning: invalid id" + t.getLocalizedMessage)
                messages.remove(m.receiptHandle)
              }
            }
          }
          Thread.sleep(10 * 1000)
        }
      }
    }

  def read(): Message[T] = {
    val m = sqsQueue.readRAW()
    visibilityExtender.addMessage(m)
    m
  }

  def init() {
    sqsQueue.init()

    if(!visibilityExtender.isAlive) {
      try {
        visibilityExtender.start()
      } catch {
        case t: IllegalThreadStateException => ()
      }
    }
    //todo create table
    
  }

  //todo throughput
  def markAsReady() {
    aws.ddb.putItem(new PutItemRequest()
      .withTableName(name)
      .withItem(readyItem)
    )
  }

  def isReady: Boolean = {
    try {
      !aws.ddb.getItem(new GetItemRequest()
        .withTableName(name)
        .withKey(readyItem)
      ).getItem.isEmpty
    } catch {
      case t: Throwable => false
    }

  }
  
  

  //need for reset state...
  def reset() {
    visibilityExtender.clear()

  }

  //think about >1 MB
  def resultsIds: Set[String] = {
    aws.ddb.scan(new ScanRequest()
      .withTableName(name)
      .withAttributesToGet(resultIdAttr)
    ).getItems.toList.map(_.get(resultIdAttr).getS).filter(!_.equals(unitId)).toSet
  }

  def tasksIds: Set[String] = {
    aws.ddb.scan(new ScanRequest()
      .withTableName(name)
      .withAttributesToGet(taskIdAttr)
    ).getItems.toList.map(_.get(taskIdAttr).getS).filter(!_.equals(readyItemId)).toSet
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
