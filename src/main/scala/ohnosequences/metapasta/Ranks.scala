package ohnosequences.metapasta

trait TaxonomyRank


object TaxonomyRank {
  val ranks = List(Genus, Phylum, Species, Class, Order, Superkingdom)
  val allRanks = List(Genus, Phylum, Species, Subspecies, Class, Order, Superkingdom, NoRank)

  def apply(rank: String): TaxonomyRank = allRanks.find { r => rank.equals(r.toString) } match {
    case None => Unknown(rank)
    case Some(taxonomyRank) => taxonomyRank
  }
}

case object Genus extends TaxonomyRank {
  override def toString: String = "genus"
}

case object Phylum extends TaxonomyRank {
  override def toString: String = "phylum"
}

case object Subspecies extends TaxonomyRank {
  override def toString: String = "subspecies"
}

case object Species extends TaxonomyRank {
  override def toString: String = "species"
}

case object Class extends TaxonomyRank {
  override def toString: String = "class"
}

case object Order extends TaxonomyRank {
  override def toString: String = "order"
}

case object Superkingdom extends TaxonomyRank {
  override def toString: String = "superkingdom"
}

case object NoRank extends TaxonomyRank {
  override def toString: String = "no rank"
}

case class Unknown(rank: String) extends TaxonomyRank {
  override def toString: String = rank
}

