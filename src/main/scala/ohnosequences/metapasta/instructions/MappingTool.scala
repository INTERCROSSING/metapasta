package ohnosequences.metapasta.instructions


import ohnosequences.awstools.s3.{LoadingManager, ObjectAddress}
import ohnosequences.logging.Logger
import ohnosequences.metapasta.databases._
import ohnosequences.metapasta._

import scala.util.{Failure, Success, Try}

import java.io.File

trait AnyMappingTool {

  type Reference <: ReferenceId

  type Database <: Database16S[Reference]

  def extractReadId(header: String): ReadId

  def launch(logger: Logger, workingDirectory: File, database: Database, readsFile: File, outputFile: File): Try[List[Hit[Reference]]]

  val name: String
}


abstract class MappingTool[R <: ReferenceId, D <: Database16S[R]] extends AnyMappingTool {
  override type Reference = R
  override type Database = D

}

