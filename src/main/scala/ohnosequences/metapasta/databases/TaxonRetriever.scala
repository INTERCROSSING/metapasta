package ohnosequences.metapasta.databases

import ohnosequences.metapasta.Taxon


trait ReferenceId {

  val id: String

 // def toRaw = RawRefId(id)

}

//for results
//case class RawRefId(id: String) extends ReferenceId

trait TaxonRetriever[R <: ReferenceId] {
  def getTaxon(referenceId: R): Option[Taxon]
}

case class GI(id: String) extends ReferenceId


trait GIMapper extends TaxonRetriever[GI] {
}


