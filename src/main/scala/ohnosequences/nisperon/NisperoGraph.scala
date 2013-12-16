package ohnosequences.nisperon

import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer
import ohnosequences.nisperon.queues.MonoidQueueAux

case class Node[N](label: N)

case class Edge[E, N](label: E, source: Node[N], target: Node[N])


class Graph[N, E](val edges: List[Edge[E, N]]) {
  val nodes: Set[Node[N]] = {
    edges.toSet.flatMap { edge: Edge[E, N] =>
      Set(edge.source, edge.target)
    }
  }

  def remove(edge: Edge[E, N]): Graph[N, E] = {
    new Graph(edges.filterNot(_.equals(edge)))
  }

  def out(node: Node[N]): Set[Edge[E, N]] = {
    edges.filter(_.source.equals(node)).toSet
  }

  def in(node: Node[N]): Set[Edge[E, N]] = {
    edges.filter(_.target.equals(node)).toSet
  }

  def sort: List[Node[N]] =  {
    var arcFree = new Graph(edges.filterNot { edge =>
      edge.source.equals(edge.target)
    })

    val result = ListBuffer[Node[N]]()
    var s: List[Node[N]] = nodes.toList.filter(arcFree.in(_).isEmpty)
    println(s)
    while(!s.isEmpty) {
      val n = s.head
      s = s.tail
      result += n

     // println("analyzing node: " + n + " in: " + arcFree.out(n))
      arcFree.out(n).foreach { e =>
        arcFree = arcFree.remove(e)
        if(!arcFree.nodes.contains(e.target) || arcFree.in(e.target).isEmpty) {
          s = e.target :: s

        }
      }
    }

    if(!arcFree.edges.isEmpty) {
     // println("oioioi")
    }
    result.toList
  }

}


class NisperoGraph(nisperos: HashMap[String, NisperoAux]) {
  val queues = {
    val r = new HashMap[String, MonoidQueueAux]()
    nisperos.values.foreach { nispero =>
      r += (nispero.inputQueue.name -> nispero.inputQueue)
      r += (nispero.outputQueue.name -> nispero.outputQueue)
    }
    r
  }

  val edges = nisperos.values.toList.map { nispero =>
    Edge(
      label = nispero.nisperoConfiguration.name,
      source = Node(nispero.inputQueue.name),
      target = Node(nispero.outputQueue.name)
    )
  }

  val graph: Graph[String, String] = new Graph(edges)

  def checkQueues() {
    val sorted = graph.sort
   // println(sorted)
    sorted.filterNot(graph.out(_).isEmpty).find { node =>
      !queues(node.label).isEmpty
    } match {
      case None => println("all queues are empty")
      case Some(node) => println("queue " + node.label + " isn't empty")
    }

//    graph.sort.filterNot(graph.out(_).isEmpty).foreach { node =>
//      queues.get(node.label) match {
//        case None => println("queue " + node.label + " not found")
//        case Some(queue) => {
//          print("checking queue: " + queue.name + " ")
//          if (queue.isEmpty) println("empty") else println("not empty")
//        }
//      }
//    }
  }

}


