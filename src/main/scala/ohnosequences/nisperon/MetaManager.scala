package ohnosequences.nisperon

import org.clapper.avsl.Logger
import ohnosequences.nisperon.queues.{Merger, SQSQueue}
import ohnosequences.awstools.s3.ObjectAddress
import java.io.File

class MetaManager(nisperon: Nisperon) {
  import nisperon._

  val logger = Logger(this.getClass)

  //todo remove notification topic

  def run() {
    try {
      logger.info("metamanager started")

      val queueName = nisperon.nisperonConfiguration.metamanagerQueue

      val controlTopic = nisperon.aws.sns.createTopic(nisperon.nisperonConfiguration.controlTopic)
      val queueWrap = nisperon.aws.sqs.createQueue(queueName)
      controlTopic.subscribeQueue(queueWrap)



      val controlQueue = new SQSQueue[ManagerCommand](aws.sqs.sqs, queueName, new JsonSerializer[ManagerCommand]())
      val reader = controlQueue.getReader(true)

      @volatile var stopped = false

      val terminationDaemon = new TerminationDaemon(nisperon)
      terminationDaemon.start()

      while(!stopped) {
        val m0 = reader.read
        logger.info("parsing message")
        val command: ManagerCommand= m0.value()
        command match {

          case ManagerCommand("undeploy", reason) => {

            stopped = true

            var solved = false
            if (reason.equals("solved")) {
             // val mergerThread = new Thread("merger") {
             //   override def run() {
                  try {
                    logger.info("merging queues")
                    nisperon.mergingQueues.foreach { queue =>
                      new Merger(queue, nisperon).merge()
                    }
                   // stopped = true
                    solved = true
                  } catch {
                    case t: Throwable => logger.error("error during merging: " + t.toString + " " + t.getMessage)
                      Nisperon.terminateInstance(nisperon.aws, nisperon.nisperonConfiguration.bucket, logger, "metamanager", t)
                  }
             //   }
             // }.start()
            }

            if(!nisperonConfiguration.removeAllQueues) {
            nisperon.checkQueues() match {
              case Right(queues) => logger.info("deleting queues"); {
                queues.foreach { q =>
                  try {
                    q.delete()
                  } catch {
                    case t: Throwable => ()
                  }
                }
              }
              case Left(queue) => logger.info(queue.name + " isn't empty")
            }
            } else {
              nisperon.nisperos.values.foreach { nispero =>
                try {
                  nispero.inputQueue.delete()
                } catch {
                  case t: Throwable => t.printStackTrace()
                }
                try {
                  nispero.outputQueue.delete()
                } catch {
                  case t: Throwable => t.printStackTrace()
                }
              }

            }



            var result: Option[String] = None

            ///todo add attempts!
            try {
              result = nisperon.undeployActions(solved)
            } catch {
              case t: Throwable => logger.error("error during performing undeploy actions " + t.toString)
                Nisperon.terminateInstance(aws, nisperon.nisperonConfiguration.bucket, logger, "metamanager", t)
            }

            result match {
              case None => nisperon.notification(nisperon.nisperonConfiguration.id + " terminated", "reason: " + reason)
              case Some(failure) =>
                nisperon.notification(nisperon.nisperonConfiguration.id + " terminated", "failure of undeploy actions:" + reason)
            }


            try {
              val instanceId = nisperon.aws.ec2.getCurrentInstanceId.getOrElse("undefined_" + System.currentTimeMillis())
              val logAddress = ObjectAddress(nisperon.nisperonConfiguration.bucket, "logs/" + "metamanager-" + instanceId)
              //todo incorporate with ami
              nisperon.aws.s3.putObject(logAddress, new File("/root/log.txt"))
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


          }
        }
        reader.reset()
      }
    } catch {
      case t: Throwable => {
        logger.error("error in metamanager " + t.toString)
        Nisperon.terminateInstance(aws, nisperon.nisperonConfiguration.bucket, logger, "metamanager", t)
      }
    }
  }

}

