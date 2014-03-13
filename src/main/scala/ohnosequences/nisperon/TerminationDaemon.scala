package ohnosequences.nisperon

import org.clapper.avsl.Logger
import ohnosequences.nisperon.queues.Merger
import ohnosequences.awstools.s3.ObjectAddress
import java.io.File

class TerminationDaemon(nisperon: Nisperon) extends Thread {
  val logger = Logger(this.getClass)

  val initLaunchTime = {

    var ititTime= 0L
    var attempt = 10
    while (ititTime== 0 && attempt > 0) {
      try {
        attempt -= 1
        ititTime = nisperon.launchTime

      } catch {
        case t: Throwable => t.printStackTrace()
      }

    }
    ititTime

  }
  val t0 = System.currentTimeMillis() / 1000

  def launchTime(): Long = {
    val t1 = System.currentTimeMillis() / 1000
    initLaunchTime + (t1 - t0)
  }

  override def run() {
    logger.info("termination daemon started")
    try {
      var stopped = false
      while (!stopped) {


        if (nisperon.nisperonConfiguration.autoTermination) {
          logger.info("checking queues")
          nisperon.checkQueues() match {
            case Left(queue) => logger.info(queue.name + " isn't empty")
            case Right(queues) => {
              logger.info("all queues are empty. terminating")
              nisperon.undeploy("solved")
              stopped = true
            }
          }
        }

        if (launchTime() > nisperon.nisperonConfiguration.timeout) {
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
