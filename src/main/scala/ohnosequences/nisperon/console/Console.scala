package ohnosequences.nisperon.console

import ohnosequences.nisperon.{NisperoAux, Nisperon}
import scala.xml.NodeSeq
import com.amazonaws.services.autoscaling.model.AutoScalingGroup
import collection.JavaConversions._

case class Console(nisperon: Nisperon) {
  def nisperos(): NodeSeq = {
    val l = for{(name, nispero) <- nisperon.nisperos }
      yield <li><a href={"/nispero/" +name}>{name}</a></li>

    l.toList

  }

  def workersProperties(group: AutoScalingGroup): NodeSeq = {
//    <tr>
//      <td>1,001</td>
//      <td>Lorem</td>
//    </tr>
//todo it's wrong because uses static information

    <tr>
      <td>auto scaling group</td>
      <td>{group.getAutoScalingGroupName}</td>
    </tr>
    <tr>
       <td>minimum size</td>
       <td>{group.getMinSize}</td>
    </tr>
    <tr>
      <td>desired capacity</td>
      <td>{group.getDesiredCapacity}</td>
    </tr>
    <tr>
      <td>maximum size</td>
      <td>{group.getMaxSize}</td>
    </tr>

  }

//  <button class="btn btn-primary btn-lg" data-toggle="modal" data-target="#terminateInstanceModal">
//    Terminate
//  </button>
  // <a class="btn btn-danger terminate" href="#" id={inst.getInstanceId}><i class="icon-refresh icon-white"></i>Terminate</a>
  def workerInstances(groups: List[AutoScalingGroup]): NodeSeq = {
    groups.flatMap { group =>
      group.getInstances.toList.map {
        inst =>
          <tr>
            <td>
              {inst.getInstanceId}
            </td>
            <td>
              {inst.getLifecycleState}
            </td>
            <td>
              <a class="btn btn-danger terminate" href="#" id={inst.getInstanceId}><i class="icon-refresh icon-white"></i>Terminate</a>

            </td>

          </tr>
      }
    }
  }
}
