package ohnosequences.metapasta

import ohnosequences.awstools.s3.ObjectAddress

trait Database {

  def install()

  //for blast
  val name: String

  def parseGI(refId: String): String
}

trait LastDatabase extends Database {}

trait BlastDatabase extends Database {}

