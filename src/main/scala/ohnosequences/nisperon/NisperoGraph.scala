package ohnosequences.nisperon

import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer
import ohnosequences.nisperon.queues.{ProductQueue, MonoidQueueAux}

case class Node[N](label: N)

case class Edge[E, N](label: E, source: Node[N], target: Node[N])


class Graph[N, E](val edges: List[Edge[E, N]]) {

  override def toString() = {
    "nodes: " + nodes + System.lineSeparator() + "edges: " + edges
  }

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
    //println(s)
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

  def flatQueue(queue: MonoidQueueAux): List[MonoidQueueAux] = {
    queue match {
      case ProductQueue(q1, q2) => flatQueue(q1) ++ flatQueue(q2)
      case q => List(q)
    }
  }

  val queues = {
    val r = new HashMap[String, MonoidQueueAux]()

    nisperos.values.foreach { nispero =>
      r ++= flatQueue(nispero.inputQueue).map{ q=>
        q.name -> q
      }
      r ++= flatQueue(nispero.outputQueue).map{ q=>
        q.name -> q
      }
    }
    r
  }

  val edges = nisperos.values.toList.flatMap { nispero =>
    for {
      i <- flatQueue(nispero.inputQueue)
      o <- flatQueue(nispero.outputQueue)
    } yield Edge(
      label = nispero.nisperoConfiguration.name,
      source = Node(i.name),
      target = Node(o.name)
    )
  }

  val graph: Graph[String, String] = new Graph(edges)

  def checkQueues(): Option[MonoidQueueAux] = {
    val sorted = graph.sort
   // println(sorted)
    sorted.filterNot(graph.out(_).isEmpty).find { node =>
      !queues(node.label).isEmpty
    } match {
      case None => println("all queues are empty"); None
      case Some(node) => println("queue " + node.label + " isn't empty"); queues.get(node.label)
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


