package ohnosequences.nisperon.queues

import ohnosequences.nisperon._
import scala.collection.JavaConversions._
import org.clapper.avsl.Logger
import ohnosequences.awstools.s3.ObjectAddress
import com.amazonaws.services.s3.model.ListObjectsRequest
import com.amazonaws.AmazonClientException

//
//trait Trackable {
//  def resultsIds: Set[String]
//
//}
//todo flush! workaround 1 thread!

//think about batch stuff latter
class S3Queue[T](aws: AWS, name: String, monoid: Monoid[T], serializer: Serializer[T]) extends MonoidQueue[T](name, monoid, serializer) {

  val logger = Logger(this.getClass)


  val sqsQueue = new SQSQueue(aws.sqs.sqs, name, stringSerializer)

  val visibilityExtender = new VisibilityExtender[String](name)

  val sqsWriter = new SQSWriter(aws, sqsQueue.queueURL, stringMonoid, name, stringSerializer)
  val s3Writer = new S3Writer(aws, monoid, name, serializer, 1)

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
      var start = System.currentTimeMillis()
      message = sqsQueue.readRAW()
      var end = System.currentTimeMillis()
      logger.info("read from sqs: " + (end - start))

      //println("taked: " + message)
      //check timeout
      try {
        message.changeMessageVisibility(100)
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
        var start: Long = 0
        var end: Long = 0
        var rawValue: String = ""
        try {
          start = System.currentTimeMillis()
          rawValue = aws.s3.readWholeObject(address)
          end = System.currentTimeMillis()
          logger.info("read from s3: " + (end - start))
        } catch {
          case t: AmazonClientException => {
            logger.warn("can't read object waiting")
            Thread.sleep(1000)
            rawValue = aws.s3.readWholeObject(address)
          }
        }


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

  def initRead() {
    init()

    sqsQueue.init()

    if (!visibilityExtender.isAlive) {
      try {
        visibilityExtender.start()
      } catch {
        case t: IllegalThreadStateException => ()
      }
    }

  }

  def init() {
    aws.s3.createBucket(name)
  }

  def initWrite() {
    init()
    sqsWriter.init()
    s3Writer.init()


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