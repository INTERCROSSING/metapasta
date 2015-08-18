package ohnosequences.metapasta.databases

import ohnosequences.metapasta.{TaxonomyRank, Taxon, Tree}

trait AnyTaxonInfo {
  val taxon: Taxon
  val parentTaxon: Option[Taxon]
  val scientificName: String
  val rank: TaxonomyRank
}

case class TaxonInfo(taxon: Taxon, parentTaxon: Option[Taxon], scientificName: String, rank: TaxonomyRank) extends AnyTaxonInfo

trait Taxonomy {
  val tree: Tree[Taxon]
  def getTaxonInfo(taxon: Taxon): AnyTaxonInfo
}
