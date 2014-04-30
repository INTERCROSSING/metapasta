package ohnosequences.logging

import ohnosequences.nisperon.{JSON, AWS}
import com.amazonaws.services.dynamodbv2.model.{AttributeValue, PutItemRequest}
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider


sealed trait Message {
  val message: String
  val typeS: String
}

object Message {
  def apply(message: String, messageType: String): Message = messageType match {
    case "E" => Error(message)
    case "W" => Warning(message)
  }
}


case class Error(message: String) extends Message {
  val typeS = "E"
}

case class Warning(message: String) extends Message {
  val typeS = "W"
}



case class Logger(aws: AWS, queueName: String, table1: String, table2: String, instanceId: String, component: String) {


  //idempotent
  //todo create table
  def init() {

  }
//assumption: no messages with same id and timestamp


  //add sync latter
  val sqs = aws.sqs
  val queue = sqs.createQueue(queueName)


  def log(message: Message) {

    queue.sendMessage(JSON.toJSON(LogMessage(message.message, message.typeS, table1, table2, component, instanceId, System.currentTimeMillis())))

   // EnvironmentVariableCredentialsProvider
//    val item = new java.util.HashMap[String, AttributeValue]()
//    item.put()
//
//    ddb.putItem(new PutItemRequest()
//      .withTableName(table)
//      .withItem(new )
//    )
  }
}
