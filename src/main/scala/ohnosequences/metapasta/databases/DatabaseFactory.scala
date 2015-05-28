//package ohnosequences.metapasta.databases
//
//import ohnosequences.logging.Logger
//import ohnosequences.metapasta.Factory
//import ohnosequences.awstools.s3.LoadingManager
//
//import scala.util.Try
//
//
//trait DatabaseFactory[+T] extends Factory[(Logger, LoadingManager), T] {
//  def build(logger: Logger, loadingManager: LoadingManager): Try[T]
//
//  override def build(ctx: (Logger, LoadingManager)): Try[T] = build(ctx._1, ctx._2)
//
//}
