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
    email = "museeer@gmail.com",
    autoTermination = false
  )

  val fasta = queue(
    name = "fastaQueue",
    monoid = stringMonoid,
    serializer = stringSerializer
  )

  val blastRes = s3queue(
    name = "blastRes",
    monoid = new ListMonoid[BlastResult],
    serializer = new JsonSerializer[List[BlastResult]]
  )

  override val mergingQueues = List(blastRes)

  val blastNispero = nispero(
    inputQueue = fasta,
    outputQueue = blastRes,
    instructions = new BlastInstructions(aws, new NTDatabase(aws)),
    nisperoConfiguration = NisperoConfiguration(nisperonConfiguration, "blast")
  )


  def undeployActions() {
    println("undeploy!")
  }

  def addTasks() {
  //  val bio4j = new Bio4jDistributionDist(blastNispero.managerDistribution.metadata)
  //  val noderetr = bio4j.nodeRetriever

    //noderetr.


    fasta.init()

    val t1 = System.currentTimeMillis()

//    for (i <- 1 to n) {
//      if(i % 100 == 0) {
//        println((n - i) + " left")
//
//      }
//      queue1.put("id" + i, List(i))
//    }

    //todo fix this order!!!
    //added 232 ms
    //check you e-mail for further instructions
    //  unprocessed:0
    fasta.put("0", List(io.Source.fromFile("f1.fasta").mkString, io.Source.fromFile("f2.fasta").mkString))


    val t2 = System.currentTimeMillis()

    println("added " + (t2-t1) + " ms")

    //should be initialized

   // checkQueues()

   // println(queue1.list())

  }
}
