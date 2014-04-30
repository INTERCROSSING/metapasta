package ohnosequences.logging

import scala.concurrent.{ExecutionContext, Promise, Future}
import scala.collection.generic.CanBuildFrom

class FancyProducers {

  def messages: List[String] = {
    Thread.sleep(1000)
    List(System.currentTimeMillis().toString, System.currentTimeMillis().toString, System.currentTimeMillis().toString, System.currentTimeMillis().toString)
  }

  def sequenceOrBailOut[A, M[_] <: TraversableOnce[_]](in: M[Future[A]] with TraversableOnce[Future[A]])(implicit cbf: CanBuildFrom[M[Future[A]], A, M[A]], executor: ExecutionContext): Future[M[A]] = {
    val p = Promise[M[A]]()

    // the first Future to fail completes the promise
    in.foreach(_.onFailure{case i => p.tryFailure(i)})

    // if the whole sequence succeeds (i.e. no failures)
    // then the promise is completed with the aggregated success
    Future.sequence(in).foreach(p trySuccess _)

    p.future
  }

  def producer(): Future[Unit] = {
    import scala.concurrent._
    import ExecutionContext.Implicits.global
    future {
      println(messages)
      producer()
    }
  }



}
