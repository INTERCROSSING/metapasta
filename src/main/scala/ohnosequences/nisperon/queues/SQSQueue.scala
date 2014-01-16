package ohnosequences.nisperon.queues

import ohnosequences.nisperon.{SNSMessage, JsonSerializer, Serializer}

import scala.collection.JavaConversions._

import org.clapper.avsl.Logger
import java.util.concurrent.ArrayBlockingQueue
import com.amazonaws.services.sqs.AmazonSQS
import com.amazonaws.services.sqs.model._


//todo change default visibility timeout
//todo check s3 writing
class SQSMessage[T](val sqs: AmazonSQS, val queueUrl: String, val id: String, val receiptHandle: String, val body: String, val serializer: Serializer[T]) extends Message[T] {

  def value(): T = {
    serializer.fromString(body)
  }

  def delete() {
   // q.VisibilityExtender.deleteMessage(m)
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
  }
}

case class ValueWrap(id: String, body: String)

object valueWrapSerializer extends JsonSerializer[ValueWrap]


class SQSQueue[T](val sqs: AmazonSQS, val name: String, val serializer: Serializer[T], snsRedirected: Boolean = false) {

  val snsMessageParser = new JsonSerializer[SNSMessage]()

  val logger = Logger(this.getClass)

  val queueURL = sqs.createQueue(new CreateQueueRequest()
    .withQueueName(name)
  ).getQueueUrl

  @volatile var stopped = false

  def terminate() {
    stopped = true
  }

  val bufferSize = 20
  val buffer = new ArrayBlockingQueue[SQSMessage[T]](bufferSize)

  object SQSReader extends Thread("sqs reader") {

    override def run() {

      while(!stopped) {
        try {
            val messages = sqs.receiveMessage(new ReceiveMessageRequest()
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
              buffer.put(new SQSMessage[T](sqs, queueURL, valueWrap.id, m.getReceiptHandle, valueWrap.body, serializer))
            }

            if(messages.isEmpty) {
              Thread.sleep(500)
            } else {
              Thread.sleep(5)
            }
        } catch {

          case t: Throwable => {
              logger.error("error during reading from queue " + name + " " + t.toString + " " + t.getMessage)
              terminate()
           // stopped = true

          }
        }
      }
    }
  }


  def init() {
    try {
      sqs.setQueueAttributes(new SetQueueAttributesRequest()
        .withQueueUrl(queueURL)
        .withAttributes(Map(QueueAttributeName.VisibilityTimeout.toString -> "10"))
      )
    } catch {
      case t: Throwable => logger.error("error during changing timeout for queue " + name + " " + t.toString)
    }

    if(!SQSReader.isAlive) {
      try {
        SQSReader.start()
      } catch {
        case t: IllegalThreadStateException => ()
      }
    }

  }

  def put(id: String, t: T): String = {
    if(stopped) throw new Error("queue is stopped")
    sqs.sendMessage(new SendMessageRequest()
      .withQueueUrl(queueURL)
      .withMessageBody(valueWrapSerializer.toString(ValueWrap(id, serializer.toString(t))))
    ).getMessageId
  }


  def read(): Message[T] = {
    if(stopped) throw new Error("queue is stopped")
    buffer.take()
  }

  def readRAW(): SQSMessage[T] = {
    if(stopped) throw new Error("queue is stopped")
    buffer.take()
  }

}
