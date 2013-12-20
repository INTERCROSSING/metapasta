package ohnosequences.nisperon

import ohnosequences.nisperon.queues._
import org.clapper.avsl.Logger
import ohnosequences.awstools.s3.ObjectAddress
import scala.collection.mutable.ListBuffer

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
    instructions.prepare()


    inputQueue.init()
    outputQueue.init()

    println("start reading messages from " + inputQueue)


    while(true) {


        // logger.info("start reading messages from: " + inputQueue.name)
      var startTime =  System.currentTimeMillis()
      //todo add multithreading here

      val messages: List[Message[inputQueue.MA]] = (1 to instructions.arity).toList.map { n =>
       // logger.info("waiting for message from: " + inputQueue.name + "[" + n + "]")
        inputQueue.read()
      }

      var endTime =  System.currentTimeMillis()

      logger.info("message read in " + (endTime - startTime))

      logger.info("executing " + instructions + " instructions on " + messages)


       try {
        startTime =  System.currentTimeMillis()
        val outputs = instructions.solve(messages.map(_.value()))
        endTime =  System.currentTimeMillis()
        logger.info("executed in " + (endTime - startTime))

        startTime =  System.currentTimeMillis()
        outputQueue.put(messages.head.id, outputs)
        endTime =  System.currentTimeMillis()
        logger.info("message written in " + (endTime - startTime))

        messages.foreach(_.delete())

       } catch {
         case t: Throwable => {
           logger.error("instructions error:")
           t.printStackTrace()
         }
       }


      }

      } catch {
        case t: Throwable =>
          //todo some reporting here!!!
          logger.error("error during processing messages: " + t.getMessage)
          t.printStackTrace()
          logger.error("terminating instance")
          //aws.ec2.getCurrentInstance.foreach(_.terminate())
          //terminate instance

      }  finally {
        inputQueue.reset()
      }

  }
}



class Worker[Input, Output, InputQueue <: MonoidQueue[Input], OutputQueue <: MonoidQueue[Output]]
(val aws: AWS, val inputQueue: InputQueue, val outputQueue: OutputQueue, val instructions: Instructions[Input, Output], val nisperoConfiguration: NisperoConfiguration) extends WorkerAux {
  type IQ = InputQueue

  type OQ = OutputQueue

}
