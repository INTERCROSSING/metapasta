package ohnosequences.nisperon


import ohnosequences.nisperon.queues._
import scala.collection.mutable
import ohnosequences.awstools.s3.ObjectAddress
import java.io.{PrintWriter, File}
import ohnosequences.nisperon.bundles.NisperonMetadataBuilder
import com.amazonaws.services.autoscaling.model.UpdateAutoScalingGroupRequest
import com.amazonaws.AmazonServiceException
import com.amazonaws.services.sqs.model.DeleteQueueRequest
import org.clapper.avsl.Logger


abstract class Nisperon {

  val nisperos = mutable.HashMap[String, NisperoAux]()

  val nisperonConfiguration: NisperonConfiguration

  val aws: AWS = new AWS()

  val logger = Logger(this.getClass)

 // val addressCreator: AddressCreator = DefaultAddressCreator

  class QueueWithDefaults[T](name: String, monoid: Monoid[T], serializer: Serializer[T]) extends
     BufferedMonoidQueue(aws, nisperonConfiguration.id + name, monoid, serializer)

  object unitQueue extends MonoidQueue[Unit]("unit", unitMonoid) {
    val unitMessage = new UnitMessage[Unit](())

    def put(value: Unit) {}

    def read(): Message[Unit] = unitMessage

    def flush() {}

    def init() {}

    def clear() {}
  }

  def queue[T](name: String, monoid: Monoid[T], serializer: Serializer[T]) = {
    new QueueWithDefaults(name, monoid, serializer)
  }

  //in secs
  def launchTime: Long = {
    if (nisperos.values.isEmpty) {
      0
    } else {
      val groupName = nisperos.values.head.nisperoConfiguration.managerGroupName
      aws.as.getCreatedTime(groupName).map(_.getTime) match {
        case Some(timestamp) => (System.currentTimeMillis() - timestamp) / 1000
        case None => 0
      }
    }
  }

  class NisperoWithDefaults[I, O, IQ <: MonoidQueue[I], OQ <: MonoidQueue[O]] (
    inputQueue: IQ, outputQueue: OQ, instructions: Instructions[I, O], nisperoConfiguration: NisperoConfiguration
  ) extends Nispero[I, O, IQ, OQ](aws, inputQueue, outputQueue, instructions, nisperoConfiguration)


  def nispero[I, O, IQ <: MonoidQueue[I], OQ <: MonoidQueue[O]] (
    inputQueue: IQ, outputQueue: OQ, instructions: Instructions[I, O], nisperoConfiguration: NisperoConfiguration
  ): Nispero[I, O, IQ, OQ] = {

    val r = new NisperoWithDefaults(inputQueue, outputQueue, instructions, nisperoConfiguration)
    nisperos.put(nisperoConfiguration.name, r)
    r
  }

  def undeployActions()

  def undeploy(reason: String) {
    val undeployMessage = JSON.toJSON(ManagerCommand("undeploy", reason))
    notification("fastapasta terminated", "reason: " + reason)
    undeployActions()

    aws.sns.createTopic(nisperonConfiguration.controlTopic).publish(undeployMessage)
  }

  def notification(subject: String, message: String) {
    val topic = aws.sns.createTopic(nisperonConfiguration.notificationTopic)
    topic.publish(message, subject)

  }

  def addTasks(): Unit

  def main(args: Array[String]) {

    args.toList match {
      case "manager" :: nisperoId :: Nil => nisperos(nisperoId).nisperoDistribution.installManager()
      case "worker" :: nisperoId :: Nil => nisperos(nisperoId).managerDistribution.installWorker()

      case "run" :: Nil => {
        //check jar
        try {
          logger.info("creating notification topic: " + nisperonConfiguration.notificationTopic)
          val topic = aws.sns.createTopic(nisperonConfiguration.notificationTopic)

          if (!topic.isEmailSubscribed(nisperonConfiguration.email)) {
            logger.info("subscribing " + nisperonConfiguration.email + " to notification topic")
            topic.subscribeEmail(nisperonConfiguration.email)
            logger.info("please confirm subscription")
          }


          aws.s3.s3.getObjectMetadata(nisperonConfiguration.artifactAddress.bucket, nisperonConfiguration.artifactAddress.key)

          nisperos.foreach {
            case (id, nispero) =>
              nispero.nisperoDistribution.runManager()
          }
          addTasks()

          println("check you e-mail for further instructions")

          notification("fastapasta started", "started")
        } catch {
          case e: AmazonServiceException if e.getErrorCode == "NoSuchKey"
            => println(nisperonConfiguration.artifactAddress + " doesn't exist: " + e.getMessage)
        }
      }

      case "add" :: "tasks" :: Nil => {
        addTasks()
      }

      case "undeploy" :: Nil => {
        //val undeployMessage = JSON.toJSON(ManagerCommand("undeploy", ""))
        //aws.sns.createTopic(nisperonConfiguration.controlTopic).publish(undeployMessage)
        undeploy("")
      }

      case "undeploy" :: "force" :: Nil => {
        nisperos.foreach {
          case (id, nispero) =>

            undeployActions()
            aws.as.deleteAutoScalingGroup(nispero.nisperoConfiguration.managerGroupName)
            aws.sqs.createQueue(nispero.nisperoConfiguration.controlQueueName).delete()


        }
      }

      case "list" :: Nil => {
        nisperos.foreach {
          case (id, nispero) => println( id + " -> " + nispero.nisperoConfiguration.workersGroupName)
        }
      }

      case "dot" :: "dot" :: Nil => {
        val dotFile = new StringBuilder()
        dotFile.append("digraph nisperon {\n")
        nisperos.foreach {
          case (id: String, nispero: NisperoAux) =>
            val i = nispero.inputQueue.name
            val o = nispero.outputQueue.name
            dotFile.append(i + " -> " + o + "[label=\"" + id + "\"]" + "\n")

        }
        dotFile.append("}\n")

        val printWriter = new PrintWriter("nisperon.dot")
        printWriter.print(dotFile.toString())
        printWriter.close()

        import sys.process._
        "dot -Tcmapx -onisperon.map -Tpng -onisperon.png nisperon.dot".!
      }

      case nispero :: "size" :: cons if nisperos.contains(nispero) => {
        val n = nisperos(nispero)
        aws.as.as.updateAutoScalingGroup(new UpdateAutoScalingGroupRequest()
          .withAutoScalingGroupName(n.nisperoConfiguration.workersGroupName)
          .withDesiredCapacity(args(2).toInt)
        )
        nisperos(nispero)
      }

      case _ => println("wrong command")

    }
  }


}
