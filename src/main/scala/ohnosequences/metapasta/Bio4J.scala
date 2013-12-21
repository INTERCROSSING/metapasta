package ohnosequences.metapasta

import ohnosequences.typesets._
import ohnosequences.statika._
import ohnosequences.statika.aws._
import ohnosequences.bio4j.distributions.Bio4jDistribution._

import ohnosequences.nisperon.bundles.{NisperonAMI, NisperonMetadataBuilder, NisperonMetadata}

class Bio4jDistributionDist(metadata2: NisperonMetadata) extends AWSDistribution(
  metadata = metadata2,
  ami = NisperonAMI,
  members = NCBITaxonomy :~: GITaxonomyIndex :~: TestTaxonomy :~: ∅
) {
  def nodeRetriever = GITaxonomyIndex.nodeRetriever
}