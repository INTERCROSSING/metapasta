package ohnosequences.metapasta

import ohnosequences.typesets._
import ohnosequences.statika._
import ohnosequences.statika.aws._
import ohnosequences.bio4j.distributions._
import Bio4jDistribution._
import ohnosequences.nisperon.bundles._

case object Bio4jDistributionDist2 extends AWSDistribution (
  metadata = (new NisperonMetadataBuilder(new generated.metadata.metapasta())).build("bio", "bio", "."),
  ami = NisperonAMI,
  members = NCBITaxonomy :~: GITaxonomyIndex :~: ∅
)

