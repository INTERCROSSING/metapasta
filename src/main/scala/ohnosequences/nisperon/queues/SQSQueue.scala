package ohnosequences.nisperon.queues

import ohnosequences.nisperon.{Serializer}

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


class SQSQueue[T](val sqs: AmazonSQS, val name: String, val serializer: Serializer[T]) {

  val logger = Logger(this.getClass)

  @volatile var queueURL = ""

  val bufferSize = 20
  val buffer = new ArrayBlockingQueue[SQSMessage[T]](bufferSize)

  object SQSReader extends Thread("sqs reader") {

    override def run() {

      while(true) {
        try {

          if(queueURL.isEmpty) {
            logger.error("queue " + name + " doesn't exist")
          } else {

            val messages = sqs.receiveMessage(new ReceiveMessageRequest()
              .withQueueUrl(queueURL)
              .withMaxNumberOfMessages(10)
            ).getMessages

            messages.foreach { m =>
              buffer.put(new SQSMessage[T](sqs, queueURL, m.getMessageId, m.getReceiptHandle, m.getBody, serializer))
            }

            if(messages.isEmpty) {
              Thread.sleep(500)
            } else {
              Thread.sleep(5)
            }
          }
        } catch {
          case t: Throwable => logger.error("queue deleted")
        }
      }
    }
  }


  def init() {
    queueURL = sqs.createQueue(new CreateQueueRequest()
      .withQueueName(name)
      .withAttributes(Map(QueueAttributeName.VisibilityTimeout.toString -> "10"))
    ).getQueueUrl

    if(!SQSReader.isAlive) {
      try {
        SQSReader.start()
      } catch {
        case t: IllegalThreadStateException => ()
      }
    }

  }

  def put(t: T): String = {
    sqs.sendMessage(new SendMessageRequest()
      .withQueueUrl(queueURL)
      .withMessageBody(serializer.toString(t))
    ).getMessageId
  }


  def read(): Message[T] = {
    buffer.take()
  }

  def readRAW(): SQSMessage[T] = {
    buffer.take()
  }

}
