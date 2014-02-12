package ohnosequences.nisperon.queues

import java.util.concurrent.ArrayBlockingQueue
import ohnosequences.nisperon.{AWS, Serializer, Monoid}
import org.clapper.avsl.Logger
import ohnosequences.awstools.s3.ObjectAddress


class S3Writer[T](aws: AWS, monoid: Monoid[T], queueName: String, serializer: Serializer[T], threads: Int = 1) {
  val batchSize = 1
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

  //todo think about flush!
  def flush() {
    if(stopped) throw new Error("queue is stopped")
    for (i <- 1 to bufferSize * 2) {
      buffer.put("id" -> monoid.unit)
    }
  }

  class WriterThread(id: Int) extends Thread("S3 writer " + id + " " + queueName) {
    override def run() {
      while(!stopped) {
        try {
            val (id, value) = buffer.take()
            if (!value.equals(monoid.unit)) {
              aws.s3.putWholeObject(ObjectAddress(queueName, id), serializer.toString(value))
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
