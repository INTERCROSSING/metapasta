package ohnosequences.metapasta

import ohnosequences.nisperon.bundles.NisperonMetadataBuilder

case class MetapastaConfiguration(
                                   metadataBuilder: NisperonMetadataBuilder,
                                   lastWorkers: Int,
                                   uploadWorkers: Option[Int],
                                   lastTemplate: String = """./lastal nt.last/$name$ reads.fastq -s 2 -T1 -f 0 -r5 -q95 -a0 -b95 -e70 -Q2 -o out.last.maf""",
                                   email: String,
                                   samples: List[PairedSample])
