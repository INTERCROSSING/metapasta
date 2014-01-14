package ohnosequences.nisperon


import ohnosequences.nisperon.queues._
import scala.collection.mutable
import ohnosequences.awstools.s3.ObjectAddress
import java.io.{PrintWriter, File}
import ohnosequences.nisperon.bundles.{WhateverBundle, NisperonMetadataBuilder}
import com.amazonaws.services.autoscaling.model.UpdateAutoScalingGroupRequest
import com.amazonaws.AmazonServiceException
import com.amazonaws.services.sqs.model.DeleteQueueRequest
import org.clapper.avsl.Logger


abstract class Nisperon {

  val nisperos = mutable.HashMap[String, NisperoAux]()

  val nisperonConfiguration: NisperonConfiguration

  val mergingQueues: List[MonoidQueueAux] = List[MonoidQueueAux]()

  val aws: AWS = new AWS()

  val logger = Logger(this.getClass)

 // val addressCreator: AddressCreator = DefaultAddressCreator

  class S3QueueLocal[T](name: String, monoid: Monoid[T], serializer: Serializer[T]) extends
     S3Queue(aws, (nisperonConfiguration.id + name).replace("_", "-").toLowerCase, monoid, serializer)

  def s3queue[T](name: String, monoid: Monoid[T], serializer: Serializer[T]) = {
    new S3QueueLocal(name, monoid, serializer)
  }

  class DynamoDBQueueLocal[T](name: String, monoid: Monoid[T], serializer: Serializer[T], writeBodyToTable: Boolean, throughputs: (Int, Int)) extends
    DynamoDBQueue(aws, nisperonConfiguration.id + name, monoid, serializer, writeBodyToTable, throughputs)

  def queue[T](name: String, monoid: Monoid[T], serializer: Serializer[T], writeBodyToTable: Boolean = true, throughputs: (Int, Int) = (100, 100)) = {
    new DynamoDBQueueLocal(name, monoid, serializer, writeBodyToTable, throughputs)
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

    logger.info("sending undeploy messages to managers")
    val undeployMessage = JSON.toJSON(ManagerCommand("undeploy", reason))

    val wrap = JSON.toJSON(ValueWrap("1", undeployMessage))

    aws.sns.createTopic(nisperonConfiguration.controlTopic).publish(wrap)
  }

  def checkQueues(): Option[MonoidQueueAux] = {
    val graph = new NisperoGraph(nisperos)
    graph.checkQueues()
  }

  def notification(subject: String, message: String) {
    val topic = aws.sns.createTopic(nisperonConfiguration.notificationTopic)
    topic.publish(message, subject)
  }

  def addTasks(): Unit

  def main(args: Array[String]) {

    args.toList match {
      case "meta" :: "meta" :: Nil => new MetaManager(Nisperon.this).run()

      case "manager" :: nisperoId :: Nil => nisperos(nisperoId).installManager()
      case "worker" :: nisperoId :: Nil => nisperos(nisperoId).installWorker()

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

          try {

            aws.s3.s3.getObjectMetadata(nisperonConfiguration.artifactAddress.bucket, nisperonConfiguration.artifactAddress.key)
          } catch {
            case t: Throwable => throw new Error("jar isn't published: " + nisperonConfiguration.artifactAddress)
          }

          logger.info("creating bucket")
          aws.s3.createBucket(nisperonConfiguration.bucket)

          nisperos.foreach {
            case (id, nispero) =>
              nispero.runManager()
          }
          addTasks()

          println("check you e-mail for further instructions")

          val bundle = new WhateverBundle(Nisperon.this, "meta", "meta")
          val userdata = bundle.userScript(bundle)

          val metagroup = nisperonConfiguration.managerGroups.autoScalingGroup(
            name = nisperonConfiguration.metamanagerGroup,
            defaultInstanceSpecs = nisperonConfiguration.defaultSpecs,
            amiId = bundle.ami.id,
            userData = userdata
          )

          aws.as.createAutoScalingGroup(metagroup)


          notification(nisperonConfiguration.id + " started", "started")
        } catch {
          case e: AmazonServiceException if e.getErrorCode == "NoSuchKey"
            => println(nisperonConfiguration.artifactAddress + " doesn't exist: " + e.getMessage)
        }
      }

      case "graph" :: Nil => {
        logger.info(new NisperoGraph(nisperos).graph)
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
        aws.as.deleteAutoScalingGroup(nisperonConfiguration.metamanagerGroup)

        nisperos.foreach {
          case (id, nispero) =>
            undeployActions()
            aws.as.deleteAutoScalingGroup(nispero.nisperoConfiguration.managerGroupName)
            aws.as.deleteAutoScalingGroup(nispero.nisperoConfiguration.workersGroupName)
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

object Nisperon {

  def terminateInstance(aws: AWS, bucket: String, logger: Logger, component: String, t: Throwable, timeout: Int = 5) {
    t.printStackTrace()

    logger.error("terminating instance")
    try {
      logger.info("waiting " + timeout + " secs")
      Thread.sleep(timeout * 1000)
      val instanceId = aws.ec2.getCurrentInstanceId.getOrElse("undefined_" + System.currentTimeMillis())
      val logAddress = ObjectAddress(bucket, "logs/" + component + "-" + instanceId)
      //todo incorporate with ami
      aws.s3.putObject(logAddress, new File("/root/log.txt"))
    } catch {
      case t: Throwable => logger.error("couldn't upload log")
    }
    aws.ec2.getCurrentInstance.foreach(_.terminate())
  }
}
