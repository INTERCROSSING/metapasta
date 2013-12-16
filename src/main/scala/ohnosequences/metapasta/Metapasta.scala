package ohnosequences.metapasta

import ohnosequences.nisperon._
import ohnosequences.nisperon.bundles.NisperonMetadataBuilder
import ohnosequences.nisperon.NisperonConfiguration

object testInstructions extends MapInstructions[Int, Int] {
  def apply(input: Int): Int = input * input
}

object Metapasta extends Nisperon {
  val nisperonConfiguration: NisperonConfiguration = NisperonConfiguration(
    metadataBuilder = new NisperonMetadataBuilder(new generated.metadata.metapasta()),
    email = "museeer@gmail.com"
  )

  val queue1 = s3queue(
    name = "queue1",
    monoid = intMonoid,
    serializer = intSerializer
  )

  val queue2 = queue(
    name = "queue2",
    monoid = intMonoid,
    serializer = intSerializer
  )

  val nispero1 = nispero(
    inputQueue = queue1,
    outputQueue = queue2,
    instructions = testInstructions,
    nisperoConfiguration = NisperoConfiguration(nisperonConfiguration, "square")
  )


  def undeployActions() {
    println("undeploy!")
  }

  def addTasks() {
    queue1.init()
    queue2.init()

    val t1 = System.currentTimeMillis()

//    for (i <- 1 to n) {
//      if(i % 100 == 0) {
//        println((n - i) + " left")
//
//      }
//      queue1.put("id" + i, List(i))
//    }

    queue1.put("0", (1 to 1000).toList)

    val t2 = System.currentTimeMillis()

    println("added " + (t2-t1) + " ms")

    //should be initialized

    checkQueues()

    println(queue1.list())

  }
}
