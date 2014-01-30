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
class S3Queue[T](aws: AWS, name: String, monoid: Monoid[T], serializer: Serializer[T], deadLetterQueueName: String) extends MonoidQueue[T](name, monoid, serializer) {

  val logger = Logger(this.getClass)

  val sqsQueue = new SQSQueue[String](aws.sqs.sqs, name, stringSerializer, deadLetterQueueName = Some(deadLetterQueueName))

  var sqsWriter: Option[SQSWriter[String]] = None
  var sqsReader: Option[SQSReader[String]] = None


  val s3Writer = new S3Writer(aws, monoid, name, serializer, 1)

  def put(taskId: String, values: List[T]) {
    sqsWriter match {
      case Some(writer) => {
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
            writer.write(id, id)
        }
        writer.flush()
      }
      case None => throw new Error("unitilized")
    }

  }


  def read(): Message[T] = {
    val message = sqsReader.get.read

    new Message[T] {
      val id: String = message.id


      def value(): T = {
        //val address = m.value()
        val address = ObjectAddress(name, id)
        logger.info("reading data from " + address)
        var start: Long = 0
        var end: Long = 0

       // try {
        start = System.currentTimeMillis()
        val rawValue = aws.s3.readWholeObject(address)
        end = System.currentTimeMillis()
        logger.info("read from s3: " + (end - start))
        start = System.currentTimeMillis()
        val t = serializer.fromString(rawValue)
        end = System.currentTimeMillis()
        logger.info("parsing: " + (end - start))

        t
      }

      def delete() {
        aws.s3.deleteObject(ObjectAddress(name, id))
        message.delete()

      }

      def changeMessageVisibility(secs: Int): Unit = message.changeMessageVisibility(secs)
    }
  }

  def initRead() {
    init()
    sqsReader = Some(sqsQueue.getReader(false))
  }

  def init() {
    aws.s3.createBucket(name)
  }

  def initWrite() {
    init()
    s3Writer.init()
    sqsWriter = Some(sqsQueue.getWriter(stringMonoid))
  }

  //need for reset state...
  def reset() {
    sqsReader.get.reset()
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