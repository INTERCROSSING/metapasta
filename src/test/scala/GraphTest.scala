package ohnosequences.fastapasta

import org.junit.Test
import org.junit.Assert._
import ohnosequences.nisperon.{Edge, Node, Graph}


class GraphTest {



  @Test
  def sort() {
    val n1 = Node(1)
    val n2 = Node(2)
    val n3 = Node(3)
    val n4 = Node(4)
    val n5 = Node(5)
    val n6 = Node(6)

    val n7 = Node(7)



    val g = new Graph(
      List(
        Edge(1, n1, n2),
        Edge(2, n2, n3),
        Edge(3, n3, n4),
        Edge(4, n2, n5),
        Edge(5, n5, n6),
        Edge(5, n7, n7)
      )
    )

    val s = g.sort()

    assertEquals(7, s.size)

    println(s)
  }

  @Test
  def sort2() {
    val n1 = Node(1)
    val n2 = Node(2)
    val n3 = Node(3)
    val n4 = Node(4)
    val n5 = Node(5)
    val n6 = Node(6)



    val g = new Graph(
      List(
        Edge(1, n1, n6),
        Edge(5, n1, n2),
        Edge(2, n2, n3),
        Edge(3, n3, n4),
        Edge(4, n5, n6),
        Edge(4, n2, n2)

      )
    )

    val s = g.sort()

    assertEquals(6, g.sort().size)

    println(g.sort())
  }
}