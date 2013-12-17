package ohnosequences.nisperon

import ohnosequences.nisperon.queues.{SQSQueue}
import org.clapper.avsl.Logger

case class SNSMessage(Message: String)

case class ManagerCommand(command: String, arg: String)

abstract class ManagerAux {

  val nisperoConfiguration: NisperoConfiguration

  val aws: AWS

  val logger = Logger(this.getClass)


  def runControlQueueHandler() {
    //it is needed for sns redirected messages
    val controlQueue = new SQSQueue[ManagerCommand](aws.sqs.sqs, nisperoConfiguration.controlQueueName, new JsonSerializer[ManagerCommand](), snsRedirected = true)
    controlQueue.init()

    val controlTopic = aws.sns.createTopic(nisperoConfiguration.nisperonConfiguration.controlTopic)
    val controlQueueWrap = aws.sqs.createQueue(controlQueue.name)
    controlTopic.subscribeQueue(controlQueueWrap)

    var stopped = false

    while(!stopped) {
      val m0 = controlQueue.read()
      val command: ManagerCommand= m0.value()

      command match {
        case ManagerCommand("undeploy", _) => {
          logger.info(nisperoConfiguration.name + " undeployed")
          stopped = true

          try {

          logger.info("delete workers group")
          aws.as.deleteAutoScalingGroup(nisperoConfiguration.workersGroupName)
          } catch {
            case t: Throwable => t.printStackTrace()
          }

          try {
          logger.info("delete control queue")
          m0.delete()
          controlQueueWrap.delete()
          } catch {
            case t: Throwable => t.printStackTrace()
          }

          try {
          logger.info("delete manager group")
          aws.as.deleteAutoScalingGroup(nisperoConfiguration.managerGroupName)
          } catch {
            case t: Throwable => t.printStackTrace()
          }

        }
        case _ => {
          logger.error(nisperoConfiguration.name + " unknown command")
        }
      }
    }
  }

}

class Manager(val aws: AWS, val nisperoConfiguration: NisperoConfiguration) extends ManagerAux {

}
