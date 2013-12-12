package ohnosequences.nisperon.queues

import ohnosequences.nisperon.{Serializer, Monoid, AWS}
import com.amazonaws.services.dynamodbv2.model._
import scala.collection.JavaConversions._
import org.clapper.avsl.Logger

//
//trait Trackable {
//  def resultsIds: Set[String]
//
//}


//think about batch stuff latter
class DynamoDBQueue[T](aws: AWS, name: String, monoid: Monoid[T], val serializer: Serializer[T]) extends MonoidQueue[T](name, monoid) {
  val taskIdAttr = "id"

  val logger = Logger(this.getClass)

  val sqsQueue = new SQSQueue[T](aws.sqs.sqs, name, serializer)
  
  val visibilityExtender = new VisibilityExtender[T](name)


  //todo batch write
  //todo throughput
  def put(taskId: String, values: List[T]) {

    values.filter(!_.equals(monoid.unit)).map {
      value =>
        val id = sqsQueue.put(value)
        aws.ddb.putItem(new PutItemRequest()
          .withTableName(name)
          .withItem(Map(taskIdAttr -> new AttributeValue().withS(id)))
        )
    }
  }


  def read(): Message[T] = {
    val m = sqsQueue.readRAW()
    visibilityExtender.addMessage(m)
    new Message[T] {
      val id: String = m.id

      def value(): T = m.value()

      def delete() {
        aws.ddb.deleteItem(new DeleteItemRequest()
          .withTableName(name)
          .withKey(Map(taskIdAttr -> new AttributeValue().withS(id)))
        )
        m.delete()
      }

      def changeMessageVisibility(secs: Int): Unit = m.changeMessageVisibility(secs)
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
    //todo create table
  }


  //need for reset state...
  def reset() {
    visibilityExtender.clear()
  }

  def isEmpty: Boolean = {
    aws.ddb.scan(new ScanRequest()
      .withTableName(name)
      .withSelect(Select.COUNT)
      .withLimit(1)
    ).getCount == 0
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
