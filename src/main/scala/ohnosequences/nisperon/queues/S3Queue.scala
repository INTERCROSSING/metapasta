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



  val sqsQueue = new SQSQueue(aws.sqs.sqs, name, stringSerializer)

  val visibilityExtender = new VisibilityExtender[String](name)

  val sqsWriter = new SQSWriter(aws, sqsQueue.queueURL, stringMonoid, name, stringSerializer)
  val s3Writer = new S3Writer(aws, monoid, name, serializer, 5)

  def put(taskId: String, values: List[T]) {
    var c = 0
    values.filter(!_.equals(monoid.unit)).map {
      value =>
        c += 1
        val id = taskId + "." + c
        s3Writer.put(id, value)
    }
    s3Writer.flush()
    c = 0
    values.filter(!_.equals(monoid.unit)).map {
      value =>
        c += 1
        val id = taskId + "." + c
        sqsWriter.put(id, id)
    }
    sqsWriter.flush()
  }


  def read(): Message[T] = {
    var message: SQSMessage[String] = null

    var taken = false
    while(!taken) {
      message = sqsQueue.readRAW()
      //println("taked: " + message)
      //check timeout
      try {
        message.changeMessageVisibility(50)
        taken = true
      } catch {
        case t: Throwable => logger.warn("skipping expired message")
      }
    }

    visibilityExtender.addMessage(message)


    new Message[T] {
      val id: String = message.id


      def value(): T = {
        //val address = m.value()
        val address = ObjectAddress(name, id)
        logger.info("reading data from " + address)
        var start = System.currentTimeMillis()
        val rawValue = aws.s3.readWholeObject(address)
        var end = System.currentTimeMillis()
        logger.info("read from s3: " + (end - start))
        start = System.currentTimeMillis()
        val t = serializer.fromString(rawValue)
        end = System.currentTimeMillis()
        logger.info("parsing: " + (end - start))
        t
      }

      def delete() {
        aws.s3.deleteObject(ObjectAddress(name, id))
        visibilityExtender.clear()
        message.delete()

      }

      def changeMessageVisibility(secs: Int): Unit = message.changeMessageVisibility(secs)
    }
  }

  def init() {
    sqsQueue.init()
    sqsWriter.init()
    s3Writer.init()

    if (!visibilityExtender.isAlive) {
      try {
        visibilityExtender.start()
      } catch {
        case t: IllegalThreadStateException => ()
      }
    }

    aws.s3.createBucket(name)
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

  def list(): List[String] = {
    aws.s3.listObjects(name).map(_.key)
  }

  def read(id: String): Option[T] = {
    try {
      aws.s3.readObject(ObjectAddress(name, id)).map(serializer.fromString)
    } catch {
      case t: Throwable => logger.warn("message not found: " + id); None
    }
  }

  def delete(id: String) {
    try {
      aws.s3.deleteObject(ObjectAddress(name, id))
    } catch {
      case t: Throwable => logger.warn("message not found: " + id); None
    }
  }
}