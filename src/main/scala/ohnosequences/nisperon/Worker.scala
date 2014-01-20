package ohnosequences.nisperon

import ohnosequences.nisperon.queues._
import org.clapper.avsl.Logger
import ohnosequences.awstools.s3.ObjectAddress
import scala.collection.mutable.ListBuffer
import java.io.File

abstract class WorkerAux {

  type IQ <: MonoidQueueAux

  type OQ <: MonoidQueueAux

  val inputQueue: IQ

  val outputQueue: OQ

  val instructions: Instructions[inputQueue.MA, outputQueue.MA]

  val nisperoConfiguration: NisperoConfiguration

//  val addressCreator: AddressCreator

  val aws: AWS

  val logger = Logger(this.getClass)


  def runInstructions() {
    try {
      logger.info("preparing instructions")
      instructions.prepare()
    } catch {
      case t: Throwable =>
        logger.error("error during preparing instructions")
        Nisperon.terminateInstance(aws, nisperoConfiguration.nisperonConfiguration.bucket, logger, "worker", t)
    }

    try {
      logger.info("initializing queues")
      inputQueue.init()
      outputQueue.init()
    } catch {
      case t: Throwable =>
        logger.error("error during initializing queues")
        Nisperon.terminateInstance(aws, nisperoConfiguration.nisperonConfiguration.bucket, logger, "worker", t)
    }

    var startTime = 0L
    var endTime = 0L

    while(true) {
      var messages = List[Message[inputQueue.MA]]()

      try {
        startTime =  System.currentTimeMillis()
        messages = (1 to instructions.arity).toList.map { n =>
         // logger.info("waiting for message from: " + inputQueue.name + "[" + n + "]")
          inputQueue.read()
        }
        endTime =  System.currentTimeMillis()
        logger.info("message read in " + (endTime - startTime))
      } catch {
        case t: Throwable => {
          logger.error("error during reading from the queue")
          Nisperon.terminateInstance(aws, nisperoConfiguration.nisperonConfiguration.bucket, logger, "worker", t)
        }
      }


      var output = List[outputQueue.MA]()
      try {
        logger.info("executing " + instructions + " instructions on " + messages.toString().take(1000))
        startTime =  System.currentTimeMillis()
        output = instructions.solve(messages.map(_.value()))
        endTime =  System.currentTimeMillis()
        logger.info("executed in " + (endTime - startTime))


        try {
          startTime =  System.currentTimeMillis()
          outputQueue.put(messages.head.id, output)
          endTime =  System.currentTimeMillis()
          logger.info("message written in " + (endTime - startTime))
          messages.foreach(_.delete())
        } catch {
          case t: Throwable => {
            logger.error("error during writing to the queue")
            Nisperon.terminateInstance(aws, nisperoConfiguration.nisperonConfiguration.bucket, logger, "worker", t)
          }
        }

        inputQueue.reset()

      } catch {
        case t: Throwable => {
          logger.error("instructions error: " + t.toString)
          t.printStackTrace()
        }
      }
    }
  }
}



class Worker[Input, Output, InputQueue <: MonoidQueue[Input], OutputQueue <: MonoidQueue[Output]]
(val aws: AWS, val inputQueue: InputQueue, val outputQueue: OutputQueue, val instructions: Instructions[Input, Output], val nisperoConfiguration: NisperoConfiguration) extends WorkerAux {
  type IQ = InputQueue

  type OQ = OutputQueue

}
