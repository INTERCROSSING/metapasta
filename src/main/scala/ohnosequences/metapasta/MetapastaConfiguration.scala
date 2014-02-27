package ohnosequences.metapasta

import ohnosequences.nisperon.bundles.NisperonMetadataBuilder
import ohnosequences.nisperon.{GroupConfiguration, Group, MapInstructions}
import ohnosequences.awstools.ec2.InstanceType
import ohnosequences.awstools.autoscaling.OnDemand

//todo fix bucket thing

sealed abstract class MappingInstructions {}
//-r10 -q95 -a0 -b95
case class Last(template: String = """./lastal nt.last/$name$ $input$ -s2 -T0 -e70 -Q$format$ -f0 -o $output$""", fasta: Boolean = false) extends MappingInstructions
case class Blast(template: String = """blastn -task megablast -db $name$ -query $input$ -out $output$ -max_target_seqs 1 -num_threads 1 -outfmt 6 -show_gis""") extends MappingInstructions

case class MetapastaConfiguration(
                                   metadataBuilder: NisperonMetadataBuilder,
                                   mappingWorkers: GroupConfiguration = Group(size = 1, max = 20, instanceType = InstanceType.M1Large, purchaseModel = OnDemand),
                                   uploadWorkers: Option[Int],
                                   email: String,
                                   samples: List[PairedSample],
                                   chunksSize: Int = 2000000,
                                   mappingInstructions: MappingInstructions = Last(),
                                   logging: Boolean = false
                                   )

//mappingWorkers = Group(size = 1, max = 20, instanceType = InstanceType.T1Micro, purchaseModel = SpotAuto)