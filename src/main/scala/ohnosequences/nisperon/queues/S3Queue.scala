package ohnosequences.nisperon.queues

import ohnosequences.nisperon._
import scala.collection.JavaConversions._
import org.clapper.avsl.Logger
import ohnosequences.awstools.s3.ObjectAddress
import com.amazonaws.services.s3.model.ListObjectsRequest

//
//trait Trackable {
//  def resultsIds: Set[String]
//
//}


//think about batch stuff latter
class S3Queue[T](aws: AWS, name: String, monoid: Monoid[T], val serializer: Serializer[T]) extends MonoidQueue[T](name, monoid) {

  val logger = Logger(this.getClass)

  val addressSerializer = new JsonSerializer[ObjectAddress]()

  val sqsQueue = new SQSQueue[ObjectAddress](aws.sqs.sqs, name, addressSerializer)

  val visibilityExtender = new VisibilityExtender[ObjectAddress](name)



  //todo batch write
  //todo throughput
  def put(taskId: String, values: List[T]) {

    var c = 0
    values.filter(!_.equals(monoid.unit)).map {
      value =>
        c += 1
        val address = ObjectAddress(name, taskId + "." + c)
        aws.s3.putWholeObject(address, serializer.toString(value))
        sqsQueue.put(address)
    }
  }


  def read(): Message[T] = {
    val m = sqsQueue.readRAW()
    visibilityExtender.addMessage(m)
    new Message[T] {
      val id: String = m.id

      def value(): T = {
        val address = m.value()
        val rawValue = aws.s3.readWholeObject(address)
        serializer.fromString(rawValue)
      }

      def delete() {
        aws.s3.deleteObject(m.value())
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
    aws.s3.s3.listObjects(new ListObjectsRequest()
      .withBucketName(name)
      .withMaxKeys(1)
    ).getObjectSummaries.isEmpty
  }


}