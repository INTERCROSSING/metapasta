package ohnosequences.nisperon

import org.clapper.avsl.Logger
import ohnosequences.nisperon.queues.Merger
import ohnosequences.awstools.s3.ObjectAddress
import java.io.File

class TerminationDaemon(nisperon: Nisperon) extends Thread {
  val logger = Logger(this.getClass)
  @volatile var stopped = false

  var mergerStarted = false

  override def run() {
    logger.info("termination daemon started")
    try {
      while(!stopped) {
        if(nisperon.nisperonConfiguration.autoTermination) {
          logger.info("checking queues")
          nisperon.checkQueues() match {
            case Some(queue) => logger.info(queue.name + " isn't empty")
            case None => {
              logger.info("all queues are empty. terminating")

              //asyn launching

              if(!mergerStarted) {
                mergerStarted = true
                val mergerThread = new Thread("merger") {
                  override def run() {
                    try {
                      logger.info("merging queues")
                      nisperon.mergingQueues.foreach { queue =>
                        new Merger(queue).merge()
                      }
                      nisperon.undeploy("solved")

                      val instanceId = nisperon.aws.ec2.getCurrentInstanceId.getOrElse("undefined_" + System.currentTimeMillis())
                      val logAddress = ObjectAddress(nisperon.nisperonConfiguration.bucket, "logs/" + "metamanager-" + instanceId)
                      //todo incorporate with ami
                      nisperon.aws.s3.putObject(logAddress, new File("/root/log.txt"))

                      stopped = true
                    } catch {
                      case t: Throwable => logger.error("error during merging: " + t.toString + " " + t.getMessage)
                      Nisperon.terminateInstance(nisperon.aws, nisperon.nisperonConfiguration.bucket, logger, "metamanager", t)
                    }
                  }
                }.start()
              }
            }
          }
        }

        if (nisperon.launchTime > nisperon.nisperonConfiguration.timeout) {
          logger.info("terminating due to timeout")
          nisperon.undeploy("timeout " + nisperon.nisperonConfiguration.timeout + " sec")
          stopped = true
        }

        Thread.sleep(5000)
      }
    } catch {
      case t: Throwable => logger.error(t.toString + " " + t.getMessage)
      Nisperon.terminateInstance(nisperon.aws, nisperon.nisperonConfiguration.bucket, logger, "metamanager", t)
    }


  }
}
