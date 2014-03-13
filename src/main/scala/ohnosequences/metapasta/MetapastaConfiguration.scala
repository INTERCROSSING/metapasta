package ohnosequences.metapasta

import ohnosequences.nisperon.bundles.NisperonMetadataBuilder
import ohnosequences.nisperon.{GroupConfiguration, Group, MapInstructions}
import ohnosequences.awstools.ec2.InstanceType
import ohnosequences.awstools.autoscaling.OnDemand

//todo fix bucket thing

//sealed abstract class MappingInstructions {}
//-r10 -q95 -a0 -b95
//case class Last(template: String = """./lastal nt.last/$name$ $input$ -s2 -T0 -e70 -Q$format$ -f0 -o $output$""", fasta: Boolean = false) extends MappingInstructions
//case class Blast(template: String, xmlOutput: Boolean) extends MappingInstructions


trait  MetapastaConfiguration {
   val metadataBuilder: NisperonMetadataBuilder
   val mappingWorkers: GroupConfiguration
   val uploadWorkers: Option[Int]
   val email: String
   val  samples: List[PairedSample]
   val chunksSize: Int
   val logging: Boolean
   val keyName: String
  val removeAllQueues: Boolean
}

case class BlastConfiguration(
                               metadataBuilder: NisperonMetadataBuilder,
                               mappingWorkers: GroupConfiguration = Group(size = 1, max = 20, instanceType = InstanceType.T1Micro, purchaseModel = OnDemand),
                               uploadWorkers: Option[Int],
                               email: String,
                               samples: List[PairedSample],
                               chunksSize: Int = 20000,
                               blastTemplate: String = """blastn -task megablast -db $name$ -query $input$ -out $output$ -max_target_seqs 1 -num_threads 1 -outfmt $out_format$ -show_gis""",
                               xmlOutput: Boolean = false,
                               database: BlastDatabase = NTDatabase,
                               logging: Boolean = true,
                               keyName: String = "nispero",
                               removeAllQueues: Boolean = true
                               ) extends MetapastaConfiguration {
}


case class LastConfiguration(
                               metadataBuilder: NisperonMetadataBuilder,
                               mappingWorkers: GroupConfiguration = Group(size = 1, max = 20, instanceType = InstanceType.M1Large, purchaseModel = OnDemand),
                               uploadWorkers: Option[Int],
                               email: String,
                               samples: List[PairedSample],
                               chunksSize: Int = 2000000,
                               lastTemplate: String = """./lastal nt.last/$name$ $input$ -s2 -T0 -e70 -Q$format$ -f0 -o $output$""",
                               useFasta: Boolean = false,
                               database: LastDatabase = NTLastDatabase,
                               logging: Boolean = true,
                               keyName: String = "nispero",
                               removeAllQueues: Boolean = true
                               ) extends MetapastaConfiguration {
}


//mappingWorkers = Group(size = 1, max = 20, instanceType = InstanceType.T1Micro, purchaseModel = SpotAuto)