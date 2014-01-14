package ohnosequences.nisperon

import org.clapper.avsl.Logger
import ohnosequences.nisperon.queues.{Merger, SQSQueue}
import ohnosequences.awstools.s3.ObjectAddress
import java.io.File

class MetaManager(nisperon: Nisperon) {
  import nisperon._

  val logger = Logger(this.getClass)


  def run() {
    try {
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
        val m0 = queue.read()
        logger.info("parsing message")
        val command: ManagerCommand= m0.value()
        command match {
          case ManagerCommand("undeploy", reason) => {

            try {
              nisperon.undeployActions()
            } catch {
              case t: Throwable => logger.error("error during performing undeploy actions " + t.toString)
              Nisperon.terminateInstance(aws, nisperon.nisperonConfiguration.bucket, logger, "metamanager", t)
            }


            try {
              logger.info("deleting auto scaling group")
              aws.as.deleteAutoScalingGroup(nisperonConfiguration.metamanagerGroup)
            } catch {
              case t: Throwable => logger.error("error during deleting autoscaling group " + t.toString)
                Nisperon.terminateInstance(aws, nisperon.nisperonConfiguration.bucket, logger, "metamanager", t)
            }

            try {
              queueWrap.delete()
            } catch {
              case t: Throwable => logger.error("error during deleting queue " + t.toString)
              Nisperon.terminateInstance(aws, nisperon.nisperonConfiguration.bucket, logger, "metamanager", t)
            }

            nisperon.notification(nisperon.nisperonConfiguration.id + " terminated", "reason: " + reason)
            stopped = true
          }
        }
      }
    } catch {
      case t: Throwable => {
        logger.error("error in metamanager " + t.toString)
        Nisperon.terminateInstance(aws, nisperon.nisperonConfiguration.bucket, logger, "metamanager", t)
      }
    }
  }

}

