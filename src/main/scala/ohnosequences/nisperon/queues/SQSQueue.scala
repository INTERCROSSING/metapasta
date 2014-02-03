package ohnosequences.nisperon.queues

import ohnosequences.nisperon._

import scala.collection.JavaConversions._

import org.clapper.avsl.Logger
import java.util.concurrent.ArrayBlockingQueue
import com.amazonaws.services.sqs.AmazonSQS
import com.amazonaws.services.sqs.model._
import java.util
import scala.Some
import ohnosequences.nisperon.SNSMessage
import ohnosequences.nisperon.queues.ValueWrap


//todo change default visibility timeout
//todo check s3 writing
class SQSMessage[T](
                     val sqs: AmazonSQS,
                     val queueUrl: String,
                     val id: String,
                     val receiptHandle: String,
                     val body: String,
                     val serializer: Serializer[T],
                     val sqsMessageId: String,
                     visibilityExtender: VisibilityExtender[T]) extends Message[T] {

  def value(): T = {
    serializer.fromString(body)
  }

  def delete() {
    println("deleting " + sqsMessageId)
   // q.VisibilityExtender.deleteMessage(m)
    visibilityExtender.deleteMessage(receiptHandle)
    sqs.deleteMessage(new DeleteMessageRequest()
      .withQueueUrl(queueUrl)
      .withReceiptHandle(receiptHandle)
    )
  }

  def changeMessageVisibility(secs: Int) {
    sqs.changeMessageVisibility(new ChangeMessageVisibilityRequest()
      .withQueueUrl(queueUrl)
      .withReceiptHandle(receiptHandle)
      .withVisibilityTimeout(secs)
    )

    println(sqsMessageId + " >> +" + secs)
  }
}

case class ValueWrap(id: String, body: String)

object valueWrapSerializer extends JsonSerializer[ValueWrap]

trait SQSReader[T] {
  def read: SQSMessage[T]
  def reset()
}

trait SQSWriter[T]  {
  def write(id: String, value: T)
  def flush()
}

//todo OverLimitException
class BufferedSQSReader[T](sqsQueue: SQSQueue[T], queueURL: String, visibilityExtender: VisibilityExtender[T], bufferSize: Int = 5, snsRedirected: Boolean = false) extends Thread("sqs reader " + sqsQueue.name) with SQSReader[T] {
  val buffer = new ArrayBlockingQueue[SQSMessage[T]](bufferSize)

  val snsMessageParser = new JsonSerializer[SNSMessage]()

  val logger = Logger(this.getClass)

  @volatile var stopped = false

  override def run() {

    while(!stopped) {
      try {
        val messages = sqsQueue.sqs.receiveMessage(new ReceiveMessageRequest()
          .withQueueUrl(queueURL)
          .withMaxNumberOfMessages(10)
        ).getMessages

        messages.foreach { m =>
          val body = if (snsRedirected) {
            snsMessageParser.fromString(m.getBody).Message
          } else {
            m.getBody
          }
          val valueWrap = valueWrapSerializer.fromString(body)
          val sqsMessage = new SQSMessage[T](sqsQueue.sqs, queueURL, valueWrap.id, m.getReceiptHandle, valueWrap.body, sqsQueue.serializer, m.getMessageId, visibilityExtender)
          buffer.put(sqsMessage)
          visibilityExtender.addMessage(sqsMessage)
        }

        if(messages.isEmpty) {
          Thread.sleep(500)
        } else {
          Thread.sleep(5)
        }
      } catch {
        case t: Throwable => {
          logger.error("error during reading from queue " + sqsQueue.name + " " + t.toString + " " + t.getMessage)
          stopped = true
        }
      }
    }
  }

  def reset() {
    //visibilityExtender.clear()
  }

  def read: SQSMessage[T] = {
    if (stopped) {
      throw new Error("sqs reader is stopped")
    } else {
      //todo fix this logic!
      var taken = false
      var message: SQSMessage[T] = null
      while (!taken) {
        message = buffer.take()
        try {
          message.changeMessageVisibility(sqsQueue.visibilityTimeout.getOrElse(100))

          taken = true
        } catch {
          case t: Throwable => () //skipping
        }
      }
     // visibilityExtender.addMessage(message)
      message
    }
  }
}

class BufferedSQSWriter[T](sqsQueue: SQSQueue[T], queueURL: String, bufferSize: Int = 20, monoid: Monoid[T]) extends Thread("sqs writer " + sqsQueue.name) with SQSWriter[T] {

  @volatile var stopped = false

  val batchSize = 10

  val logger = Logger(this.getClass)

  val buffer = new ArrayBlockingQueue[(String, T)](bufferSize)

  override def run() {
    while(!stopped) {
      try {
        val entries = new java.util.ArrayList[SendMessageBatchRequestEntry]()
        for (i <- 1 to batchSize) {
          val (id, value) = buffer.take()
          if (!value.equals(monoid.unit)) {

            val valueWrap = ValueWrap(id, sqsQueue.serializer.toString(value))
            entries.add(new SendMessageBatchRequestEntry()
              .withId(i.toString)
              .withMessageBody(valueWrapSerializer.toString(valueWrap))
            )
          }
        }

        if(!entries.isEmpty) {
          sqsQueue.sqs.sendMessageBatch(new SendMessageBatchRequest()
            .withQueueUrl(queueURL)
            .withEntries(entries)
          )
        } else {
          //logger.warn("skipping empty batch")
        }

      } catch {
        case t: Throwable =>
          logger.error(t.toString + " " + t.getMessage)
          stopped = true
      }
    }
  }


  def write(id: String, value: T) {
    if (stopped) {
      throw new Error("writter is stopped")
    } else {
      buffer.put(id -> value)
    }
  }

  def flush() {
    for (i <- 1 to bufferSize * 2) {
      buffer.put("id" -> monoid.unit)
    }
  }

}


class SQSQueue[T](val sqs: AmazonSQS, val name: String, val serializer: Serializer[T], deadLetterQueueName: Option[String] = None, val visibilityTimeout: Option[Int] = None) {

  val logger = Logger(this.getClass)

  def getReader(snsRedirected: Boolean = false): SQSReader[T] = {
    val queueURL = createQueue()
    val visibilityExtender = new VisibilityExtender[T](SQSQueue.this)
    visibilityExtender.start()
    val reader = new BufferedSQSReader[T](SQSQueue.this, queueURL, visibilityExtender, snsRedirected = snsRedirected)
    reader.start()
    reader
  }

  def getWriter(monoid: Monoid[T]): SQSWriter[T] = {
    val queueURL = createQueue()
    val writer = new BufferedSQSWriter[T](SQSQueue.this, queueURL, monoid = monoid)
    writer.start()
    writer
  }


  //return url
  def createQueue(): String = {
    val attributes = new util.HashMap[String, String]()
    deadLetterQueueName.flatMap(new SQSQueue[Unit](sqs, _, unitSerializer).getARN) match {
    //deadLetterQueueARN match {
      case Some(arn) => attributes.put("RedrivePolicy", redrivePolicyGenerator(3, arn))
      case None => () //todo ensure that queue is ok
    }
    visibilityTimeout match {
      case Some(t) => attributes.put(QueueAttributeName.VisibilityTimeout.toString, t.toString)
      case None => ()
    }
    println(attributes)

    sqs.createQueue(new CreateQueueRequest()
      .withQueueName(name)
      .withAttributes(attributes)
    ).getQueueUrl
  }

  def redrivePolicyGenerator(maxReceiveCount: Int, deadLetterTargetArn: String) = {
    """{"maxReceiveCount":"$maxReceiveCount$", "deadLetterTargetArn":"$deadLetterTargetArn$"}"""
      .replace("$maxReceiveCount$", maxReceiveCount.toString)
      .replace("deadLetterTargetArn", deadLetterTargetArn)
  }

  def getARN: Option[String] = {
    getURL.map { url =>
      sqs.getQueueAttributes(
        new GetQueueAttributesRequest()
          .withQueueUrl(url)
          .withAttributeNames(QueueAttributeName.QueueArn)
      ).getAttributes.get(QueueAttributeName.QueueArn)
    }
  }

  def getURL: Option[String] = {
    try {
      Some(sqs.getQueueUrl(new GetQueueUrlRequest().withQueueName(name)).getQueueUrl)
    } catch {
      case t: QueueDoesNotExistException => None
    }
  }

  def delete() {
    getURL.foreach { url =>
      sqs.deleteQueue(new DeleteQueueRequest().withQueueUrl(url))
    }
  }
}
