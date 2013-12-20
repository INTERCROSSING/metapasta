package ohnosequences.nisperon.queues

import ohnosequences.nisperon.{SNSMessage, JsonSerializer, Serializer}

import scala.collection.JavaConversions._

import org.clapper.avsl.Logger
import java.util.concurrent.ArrayBlockingQueue
import com.amazonaws.services.sqs.AmazonSQS
import com.amazonaws.services.sqs.model._



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

  @volatile var queueURL = sqs.createQueue(new CreateQueueRequest()
    .withQueueName(name)
  ).getQueueUrl

  val bufferSize = 20
  val buffer = new ArrayBlockingQueue[SQSMessage[T]](bufferSize)

  object SQSReader extends Thread("sqs reader") {

    override def run() {
      var stopped = false

      while(!stopped) {
        try {

          if(queueURL.isEmpty) {
            logger.error("queue " + name + " doesn't exist")
          } else {

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
          }
        } catch {
          //todo fail thing
          case t: Throwable => {
            try {
              logger.error("error during reading from queue " + name + " " + t.toString + " " + t.getMessage)
              Thread.sleep(5000)
              queueURL = sqs.createQueue(new CreateQueueRequest()
                .withQueueName(name)
              ).getQueueUrl
            } catch {
              case t: Throwable => Thread.sleep(10000)
            }
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
    sqs.sendMessage(new SendMessageRequest()
      .withQueueUrl(queueURL)
      .withMessageBody(valueWrapSerializer.toString(ValueWrap(id, serializer.toString(t))))
    ).getMessageId
  }


  def read(): Message[T] = {


    buffer.take()
  }

  def readRAW(): SQSMessage[T] = {
    buffer.take()
  }

}
