package ohnosequences.metapasta.databases

import java.io.File


trait Database16S[R <: ReferenceId] {
  def location: File

  def name: String

  def parseRawId(rawReferenceId: String): Option[R]
}

trait LastDatabase16S[R <: ReferenceId] extends Database16S[R] {}

trait BlastDatabase16S[R <: ReferenceId] extends Database16S[R] {
  def blastParameter: String = new File(location, name).getAbsolutePath
}

