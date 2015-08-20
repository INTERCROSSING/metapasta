package ohnosequences.metapasta

trait AnyTaxonInfo {
  val taxon: Taxon
  val parentTaxon: Option[Taxon]
  val scientificName: String
  val rank: TaxonomyRank
}

case class TaxonInfo(taxon: Taxon, parentTaxon: Option[Taxon] = None, scientificName: String = "", rank: TaxonomyRank = NoRank) extends AnyTaxonInfo

trait Taxonomy {
  val tree: Tree[Taxon]
  def getTaxonInfo(taxon: Taxon): Option[TaxonInfo]
}

case class Taxon(taxId: String)

