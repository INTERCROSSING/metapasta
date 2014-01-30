package ohnosequences.nisperon

import org.clapper.avsl.Logger
import ohnosequences.nisperon.queues.Merger
import ohnosequences.awstools.s3.ObjectAddress
import java.io.File

//todo fix termination
class TerminationDaemon(nisperon: Nisperon) extends Thread {
  val logger = Logger(this.getClass)


  override def run() {
    logger.info("termination daemon started")
    try {
      var stopped = false
      while (!stopped) {

        if (nisperon.nisperonConfiguration.autoTermination) {
          logger.info("checking queues")
          nisperon.checkQueues() match {
            case Some(queue) => logger.info(queue.name + " isn't empty")
            case None => {
              logger.info("all queues are empty. terminating")
              nisperon.undeploy("solved")
              stopped = true
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
