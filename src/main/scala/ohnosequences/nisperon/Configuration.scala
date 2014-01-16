package ohnosequences.nisperon

import ohnosequences.awstools.ec2.{InstanceSpecs, InstanceType}
import ohnosequences.awstools.autoscaling._
import ohnosequences.awstools.s3.ObjectAddress
import ohnosequences.statika._
import ohnosequences.nisperon.bundles.NisperonMetadataBuilder
import scala.Some
import org.clapper.avsl.Logger


case class NisperonConfiguration(metadataBuilder: NisperonMetadataBuilder, email: String, managerGroups: GroupConfiguration = SingleGroup(InstanceType.T1Micro, OnDemand), timeout: Int = 3600, autoTermination: Boolean = true) {

  def controlTopic: String = metadataBuilder.id + "controlTopic"

  def artifactAddress = metadataBuilder.jarAddress

  def notificationTopic: String = "fastanotification" + email.hashCode

  val defaultSpecs = InstanceSpecs(
    instanceType = InstanceType.T1Micro,
    amiId = "",
    securityGroups = List("nispero"),
    keyName = "nispero",
    instanceProfile = Some("nispero"),
    deviceMapping = Map("/dev/xvdb" -> "ephemeral0")
  )

  def id = metadataBuilder.id

  def metamanagerQueue = metadataBuilder.id + "_metamanager"
  def metamanagerGroup = metadataBuilder.id + "_metamanager"

  def bucket = metadataBuilder.id.replace("_", "-").toLowerCase




}


abstract class GroupConfiguration {
  val instanceType: InstanceType
  val min: Int
  val max: Int
  val size: Int
  val purchaseModel: PurchaseModel


  def autoScalingGroup(name: String, defaultInstanceSpecs: InstanceSpecs, amiId: String, userData: String): AutoScalingGroup = {
    val launchingConfiguration = LaunchConfiguration(
      name = name,
      purchaseModel = purchaseModel,
      instanceSpecs = defaultInstanceSpecs.copy(
        instanceType = instanceType,
        amiId = amiId,
        userData = userData
      )
    )

    AutoScalingGroup(
      name = name,
      launchingConfiguration = launchingConfiguration,
      minSize = min,
      maxSize = max,
      desiredCapacity = size
    )

  }
}

case class SingleGroup(
  instanceType: InstanceType = InstanceType.T1Micro,
  purchaseModel: PurchaseModel = OnDemand
) extends GroupConfiguration {
  val min = 1
  val max = 1
  val size = 1
}

case class Group(
  instanceType: InstanceType = InstanceType.T1Micro,
  min: Int = 0,
  max: Int = 10,
  size: Int,
  purchaseModel: PurchaseModel = OnDemand
) extends GroupConfiguration



case class NisperoConfiguration(nisperonConfiguration: NisperonConfiguration, name: String, workerGroup: GroupConfiguration = SingleGroup()) {

  //todo rename to worker ????
  def workersGroupName = nisperonConfiguration.id  + "_" + name + "_workers"

  def managerGroupName = nisperonConfiguration.id + "_" + name + "_manager"

  def controlQueueName = nisperonConfiguration.id + "_" + name + "_control_queue"

}


