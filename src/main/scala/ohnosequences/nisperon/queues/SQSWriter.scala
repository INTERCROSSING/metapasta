package ohnosequences.nisperon.queues

import java.util.concurrent.ArrayBlockingQueue
import ohnosequences.nisperon.{AWS, Serializer, Monoid}
import com.amazonaws.services.sqs.model.{SendMessageBatchRequest, SendMessageBatchRequestEntry}
import org.clapper.avsl.Logger


class SQSWriter[T](aws: AWS, queueUrl: String, monoid: Monoid[T], queueName: String, serializer: Serializer[T], threads: Int = 1) {
  val batchSize = 10
  val bufferSize = batchSize * (threads + 1)
  val buffer = new ArrayBlockingQueue[(String, T)](bufferSize)

  @volatile var stopped = false
  @volatile var launched = false

  val logger = Logger(this.getClass)

  def put(id: String, value: T) {
    buffer.put(id -> value)
  }

  def init() {
    if(!launched) {
      launched = true
      for (i <- 1 to threads) {
        new WriterThread(i).start()
      }
    }
  }

  def terminate() {
    stopped = true
  }

  def flush() {
    for (i <- 1 to bufferSize) {
      buffer.put("id" -> monoid.unit)
    }
  }

  class WriterThread(id: Int) extends Thread("SQS writer " + id + " " + queueName) {
    override def run() {
      while(!stopped) {
        try {
          val entries = new java.util.ArrayList[SendMessageBatchRequestEntry]()
          for (i <- 1 to batchSize) {
            val (id, value) = buffer.take()
            if (!value.equals(monoid.unit)) {

              val valueWrap = ValueWrap(id, serializer.toString(value))
              entries.add(new SendMessageBatchRequestEntry()
                .withId(i.toString)
                .withMessageBody(valueWrapSerializer.toString(valueWrap))
              )


            }
          }

            if(!entries.isEmpty) {
              aws.sqs.sqs.sendMessageBatch(new SendMessageBatchRequest()
                .withQueueUrl(queueUrl)
                .withEntries(entries)
              )
            } else {
              //logger.warn("skipping empty batch")
            }

        } catch {
          case t: Throwable =>
            logger.error(t.toString + " " + t.getMessage)
            terminate()
        }
      }
    }
  }
}
