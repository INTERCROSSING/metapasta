package ohnosequences.nisperon.queues

import java.util.concurrent.ArrayBlockingQueue
import ohnosequences.nisperon.{AWS, Serializer, Monoid}
import org.clapper.avsl.Logger
import com.amazonaws.services.dynamodbv2.model._
import scala.collection.JavaConversions._


class DynamoDBWriter[T](aws: AWS, monoid: Monoid[T], queueName: String, serializer: Serializer[T], idAttr: String, valueAttr: String, writeBodyToTable: Boolean, threads: Int = 1) {
  val batchSize = 25
  val bufferSize = batchSize * (threads + 1)
  val buffer = new ArrayBlockingQueue[(String, T)](bufferSize)

  @volatile var stopped = false
  @volatile var launched = false

  val logger = Logger(this.getClass)

  def put(id: String, value: T) {
    if(stopped) throw new Error("queue is stopped")
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
    if(stopped) throw new Error("queue is stopped")
    for (i <- 1 to bufferSize) {
      buffer.put("id" -> monoid.unit)
    }
  }

  class WriterThread(id: Int) extends Thread("DynamoDB writer " + id + " " + queueName) {
    override def run() {
      while(!stopped) {
        try {
          val writeOperations = new java.util.ArrayList[WriteRequest]()
          for (i <- 1 to 25) {
            val (id, value) = buffer.take()
            if (!value.equals(monoid.unit)) {

              val item = if(writeBodyToTable) {
                Map(
                  idAttr -> new AttributeValue().withS(id),
                  valueAttr -> new AttributeValue().withS(serializer.toString(value))
                )
              } else {
                Map(
                  idAttr -> new AttributeValue().withS(id)
                )
              }
              writeOperations.add(new WriteRequest()
                .withPutRequest(new PutRequest()
                .withItem(item)
              ))
            }
          }

          if (!writeOperations.isEmpty) {
            var operations: java.util.Map[String, java.util.List[WriteRequest]] = Map(queueName -> writeOperations)
            do {
              //to
              try {
                val res = aws.ddb.batchWriteItem(new BatchWriteItemRequest()
                  .withRequestItems(operations)
                )
                operations = res.getUnprocessedItems

                val size = operations.values().map(_.size()).sum
                println("unprocessed:" + size)
              } catch {
                case t: ProvisionedThroughputExceededException => logger.warn(t.toString + " " + t.getMessage)
              }
            } while (!operations.isEmpty)
          }
        } catch {
          case t: Throwable => {
            logger.warn(t.toString + " " + t.getMessage)
            terminate()
          }
        }
      }
    }
  }
}
