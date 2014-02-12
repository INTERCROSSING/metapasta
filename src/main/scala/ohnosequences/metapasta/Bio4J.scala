package ohnosequences.metapasta

import ohnosequences.typesets._
import ohnosequences.statika._
import ohnosequences.statika.aws._
import ohnosequences.bio4j.bundles._
import ohnosequences.nisperon.bundles._

class Bio4jDistributionDist(metadataBuilder: NisperonMetadataBuilder) extends AWSDistribution (
  metadata = metadataBuilder.build("bio", "bio", "."),
  ami = NisperonAMI,
  members = NCBITaxonomyDistribution :~: ∅
)

trait NodeRetriever {
  var nodeRetriever: com.ohnosequences.bio4j.titan.model.util.NodeRetrieverTitan
}

