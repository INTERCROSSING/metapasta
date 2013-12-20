package ohnosequences.nisperon

import org.clapper.avsl.Logger

class TerminationDaemon(nisperon: Nisperon) extends Thread {
  val logger = Logger(this.getClass)
  @volatile var stopped = false



  override def run() {
    logger.info("termination daemon started")
    while(!stopped) {
      try {

        if(nisperon.nisperonConfiguration.autoTermination) {
          logger.info("checking queues")
          nisperon.checkQueues() match {
            case Some(queue) => logger.info(queue.name + " isn't empty")
            case None => {
              logger.info("terminating")
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
      } catch {
        case t: Throwable => logger.error(t.toString + " " + t.getMessage)
      }
    }
  }
}
