//package ohnosequences.metapasta.databases
//
//import ohnosequences.logging.Logger
//import ohnosequences.awstools.s3.{LoadingManager, ObjectAddress}
//import java.io.File
//
//import scala.util.Try
//

//object Last16SFactory extends DatabaseFactory[LastDatabase16S] {
//
//
//  class LastDatabase extends LastDatabase16S {
//    val name: String = "nt.march.14.last/nt.march.14"
//    //gi|313494140|gb|GU939576.1|
//    val re = """\Qgi|\E(\d+)[^\d]+.*""".r
//    def parseGI(refId: String): Option[String] = {
//      refId match {
//        case re(id) => Some(id)
//        case s => None
//      }
//    }
//  }
//  override def build(logger: Logger, loadingManager: LoadingManager): Try[LastDatabase16S] = {
//    Try {
//      logger.info("downloading database")
//      loadingManager.downloadDirectory(ObjectAddress("metapasta", "nt.march.14.last"), new File("."))
//      new LastDatabase()
//    }
//  }
//}
