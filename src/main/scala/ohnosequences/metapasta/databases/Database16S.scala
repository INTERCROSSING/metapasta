package ohnosequences.metapasta.databases


trait Database16S[R <: ReferenceId] {
  def name: String
  def parseRawId(rawReferenceId: String): Option[R]
}

trait LastDatabase16S[R <: ReferenceId] extends Database16S[R] {}

trait BlastDatabase16S[R <: ReferenceId] extends Database16S[R] {}

