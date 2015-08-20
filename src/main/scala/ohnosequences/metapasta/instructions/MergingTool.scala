package ohnosequences.metapasta.instructions

import java.io.File

import ohnosequences.awstools.s3.{LoadingManager, ObjectAddress}
import ohnosequences.logging.Logger
import ohnosequences.metapasta.{ReadStatsBuilder, ReadsStats}

import scala.util.{Failure, Try}

abstract class MergingTool {
  val name: String

  def merge(logger: Logger, workingDirectory: File, reads1: File, reads2: File): Try[MergeToolResult]
}

case class MergeStat(total: Long = 0, merged: Long = 0, notMerged: Long = 0)

case class MergeToolResult(
                            merged: File,
                            notMerged1: Option[File],
                            notMerged2: Option[File],
                            stats: ReadsStats
                            )


