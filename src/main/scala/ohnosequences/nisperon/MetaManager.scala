package ohnosequences.nisperon

import org.clapper.avsl.Logger
import ohnosequences.nisperon.queues.{Merger, SQSQueue}

class MetaManager(nisperon: Nisperon) {
  import nisperon._

  val logger = Logger(this.getClass)

  def run() {
    logger.info("metamanager started")
    val queueName = nisperon.nisperonConfiguration.metamanagerQueue

    val controlTopic = nisperon.aws.sns.createTopic(nisperon.nisperonConfiguration.controlTopic)
    val queueWrap = nisperon.aws.sqs.createQueue(queueName)
    controlTopic.subscribeQueue(queueWrap)

    val queue = new SQSQueue[ManagerCommand](aws.sqs.sqs, queueName, new JsonSerializer[ManagerCommand](), snsRedirected = true)
    queue.init()

    @volatile var stopped = false

    val terminationDaemon = new TerminationDaemon(nisperon)
    terminationDaemon.start()

    while(!stopped) {
      try {
      val m0 = queue.read()
      logger.info("parsing message")
      val command: ManagerCommand= m0.value()
      command match {
        case ManagerCommand("undeploy", reason) => {

          try {
            nisperon.undeployActions()
          } catch {
            case t: Throwable => logger.error("error during performing undeploy actions " + t.toString)
          }

          logger.info("merging queues")
            mergingQueues.foreach { queue =>
              new Merger(queue).merge()
            }

          try {
            logger.info("deleting auto scaling group")
            aws.as.deleteAutoScalingGroup(nisperonConfiguration.metamanagerGroup)
          } catch {
            case t: Throwable => logger.error("error during deleting autoscaling group " + t.toString)
          }

          try {
            queueWrap.delete()
          } catch {
            case t: Throwable => logger.error("error during deleting queue " + t.toString)
          }

          nisperon.notification(nisperon.nisperonConfiguration.id + " terminated", "reason: " + reason)
          stopped = true
        }
      }
      } catch {
        case t: Throwable => println(t)
      }
    }
  }

}

