package ohnosequences.logging

import ohnosequences.nisperon.{JSON, AWS}
import com.amazonaws.services.dynamodbv2.model._
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import ohnosequences.logging.LogMessage
import scala.collection.JavaConversions._
import com.twitter.finatra.{Controller, FinatraServer}


//read messages from SQS and upload it to DynamoDB
case class LogUploader(aws: AWS, queueName: String) {

  val hashKey = "h"
  val rangeKey = "r"
  val bodyKey = "b"
  val typeKey = "t"

  def inverseTime(time: Long): Long = 9000000000000L - time

  //9000000000000
  //not needed so far
  def formatTime(time: String): String = {
    if (time.length == 13) {
      time
    } else {
      formatTime("0" + time)
    }
  }


  //to tables
  //1: comp:inv(timestamp) id body
  //2:

  def hashKey1(message: LogMessage): String = {
    message.component + ":" + inverseTime(message.timestamp)
  }

  def rangeKey1(message: LogMessage): String = {
    message.instanceId
  }

  def prepareItem1(message: LogMessage): java.util.HashMap[String, AttributeValue] = {
    val msg = Message(message.message, message.mType)
    val r = new java.util.HashMap[String, AttributeValue]()
    r.put(hashKey, new AttributeValue().withS(hashKey1(message)))
    r.put(rangeKey, new AttributeValue().withS(rangeKey1(message)))
    r.put(typeKey, new AttributeValue().withS(msg.typeS))
    r.put(bodyKey, new AttributeValue().withS(msg.message))
    r
  }

  def hashKey2(message: LogMessage): String = {
    message.component + ":" + message.instanceId
  }

  def rangeKey2(message: LogMessage): String = {
    String.valueOf(inverseTime(message.timestamp))
  }


  def prepareItem2(message: LogMessage): java.util.HashMap[String, AttributeValue] = {
    val msg = Message(message.message, message.mType)
    val r = new java.util.HashMap[String, AttributeValue]()
    r.put(hashKey, new AttributeValue().withS(hashKey2(message)))
    r.put(rangeKey, new AttributeValue().withS(rangeKey2(message)))
    r.put(typeKey, new AttributeValue().withS(msg.typeS))
    r.put(bodyKey, new AttributeValue().withS(msg.message))
    r
  }

  val sqs = aws.sqs
  val queue = sqs.getQueueByName(queueName).get

  def messages: List[ohnosequences.awstools.sqs.Message] = {
    // Thread.sleep(100)
    try {
      queue.receiveMessages(10)
    } catch {
      case t: Throwable => List[ohnosequences.awstools.sqs.Message]()
    }
  }

  val n = 25

  //"main loop"
  def producer(parsed: List[ohnosequences.awstools.sqs.Message]): Future[Unit] = {
    import scala.concurrent._
    import ExecutionContext.Implicits.global

    if (parsed.size > n) {
      val (left, right) = parsed.splitAt(n)
      println(Thread.currentThread().getName +  " " + "chunk: " + left.size)
      future {
        uploadToDynamoDB(left)
      }
      producer(right)
    } else {
      future {
        //println("*")
        messages
      }.flatMap[Unit] {
        case Nil => {
          if (!parsed.isEmpty) {
            uploadToDynamoDB(parsed)
          }
          producer(List[ohnosequences.awstools.sqs.Message]())
        }
        case msgs => producer(parsed ++ msgs)
      }
    }
  }

  //todo do it in parallel
  //it skips last messages???
  def uploadToDynamoDB(msgs: List[ohnosequences.awstools.sqs.Message]) {
   // println(Thread.currentThread().getName +  " upload..." + msgs.size)
    //create put request
    val writeOperations1 = new java.util.ArrayList[WriteRequest]()
    val writeOperations2 = new java.util.ArrayList[WriteRequest]()
    var table1 = ""
    var table2 = ""
   // println(Thread.currentThread().getName +  " " + writeOperations1.size)
    msgs.foreach { msg =>
      try {
    //  println(Thread.currentThread().getName +  " inside")
    //  println(Thread.currentThread().getName +  " inside0")
        val message: LogMessage =  JSON.extract[LogMessage](msg.body)

       // println(Thread.currentThread().getName +  " inside1")
          table1 = message.table1
          table2 = message.table2

      //  println(Thread.currentThread().getName +  " inside2")

          writeOperations1.add(new WriteRequest()
            .withPutRequest(new PutRequest()
            .withItem(prepareItem1(message))
            )
          )
      //  println(Thread.currentThread().getName +  " inside3")

          writeOperations2.add(new WriteRequest()
            .withPutRequest(new PutRequest()
            .withItem(prepareItem2(message))
            )
          )

      } catch {
        case t: Throwable => t.printStackTrace()
      }
    }
    println(writeOperations1.size())

    if (!writeOperations1.isEmpty) {
      println("writing ...")
      var operations: java.util.Map[String, java.util.List[WriteRequest]] = Map(table1 -> writeOperations1)
      do {
        //to
        try {
          val res = aws.ddb.batchWriteItem(new BatchWriteItemRequest()
            .withRequestItems(operations)
          )
          operations = res.getUnprocessedItems
          val size = operations.values().map(_.size()).sum

        } catch {
          case t: ProvisionedThroughputExceededException => t.printStackTrace()
        }
      } while (!operations.isEmpty)
    }

    if (!writeOperations2.isEmpty) {
     // println("writing ...")
      var operations: java.util.Map[String, java.util.List[WriteRequest]] = Map(table2 -> writeOperations2)
      do {
        //to
        try {
          val res = aws.ddb.batchWriteItem(new BatchWriteItemRequest()
            .withRequestItems(operations)
          )
          operations = res.getUnprocessedItems
          val size = operations.values().map(_.size()).sum

        } catch {
          case t: ProvisionedThroughputExceededException => t.printStackTrace()
        }
      } while (!operations.isEmpty)
    }
    msgs.foreach(queue.deleteMessage)
  }


}


object LogTest {
  def main(args: Array[String]) {
    val aws = new AWS()
    val logger = new Logger(aws, "log", "log1", "log2", "instance0", "test")
    for (i <- 1 to 0) {
      logger.log(Error("error " + i))
    }
    val logUploader = new LogUploader(aws, "log")
    //Await.ready(logUploader.producer(List[ohnosequences.awstools.sqs.Message]()), 500 seconds)
    new App(logger).main()
  }
}

class HelloWorld(logger: Logger) extends Controller {

  val console = new LogConsole(logger.aws, logger.table1, logger.table2)

  get("/log/:component/:instance") { request =>
    val component = request.routeParams("component")
    val instance = request.routeParams("instance")
   // render.plain("component: " + component + " instance: " + instance).toFuture

    render.html(console.getLastMessages(component, instance).map{ m => "<p>" + m + "</p>"
    }.mkString).toFuture
  }

}

class App(logger: Logger)  extends FinatraServer {
  register(new HelloWorld(logger))
}