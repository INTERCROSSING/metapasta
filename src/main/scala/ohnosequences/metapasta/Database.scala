package ohnosequences.metapasta

import ohnosequences.awstools.s3.ObjectAddress
import ohnosequences.nisperon.AWS

trait Database {

  def install(aws: AWS)

  //for blast
  val name: String

  def parseGI(refId: String): String
}

trait LastDatabase extends Database {}

trait BlastDatabase extends Database {}

