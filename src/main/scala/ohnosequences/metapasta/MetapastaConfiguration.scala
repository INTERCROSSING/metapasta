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
   val password: String
   val samples: List[PairedSample]
   val chunksSize: Int
   val logging: Boolean
   val keyName: String
   val removeAllQueues: Boolean
   val timeout: Int
   val mergeQueueThroughput: MergeQueueThroughput
   val generateDot: Boolean
}

abstract class MergeQueueThroughput

case class Fixed(n: Int) extends MergeQueueThroughput
case class SampleBased(ration: Double, max: Int = 100) extends MergeQueueThroughput

case class BlastConfiguration(
                               metadataBuilder: NisperonMetadataBuilder,
                               mappingWorkers: GroupConfiguration = Group(size = 1, max = 20, instanceType = InstanceType.T1Micro, purchaseModel = OnDemand),
                               uploadWorkers: Option[Int],
                               email: String,
                               samples: List[PairedSample],
                               chunksSize: Int = 20000,
                               blastTemplate: String = """blastn -task megablast -db $name$ -query $input$ -out $output$ -max_target_seqs 1 -num_threads 1 -outfmt $out_format$ -show_gis""",
                               xmlOutput: Boolean = false,
                               password: String,
                               database: BlastDatabase = NTDatabase,
                               logging: Boolean = true,
                               keyName: String = "nispero",
                               removeAllQueues: Boolean = true,
                               timeout: Int = 72000,
                               mergeQueueThroughput: MergeQueueThroughput = SampleBased(1),
                               generateDot: Boolean = true
                               ) extends MetapastaConfiguration {
}


case class LastConfiguration(
                               metadataBuilder: NisperonMetadataBuilder,
                               mappingWorkers: GroupConfiguration = Group(size = 1, max = 20, instanceType = InstanceType.M1Large, purchaseModel = OnDemand),
                               uploadWorkers: Option[Int],
                               email: String,
                               samples: List[PairedSample],
                               chunksSize: Int = 2000000,
                               lastTemplate: String = """./lastal $db$ $input$ -s2 -m100 -T0 -e70 -Q$format$ -f0 -o $output$""",
                               useFasta: Boolean = true,
                               database: LastDatabase = NTLastDatabase,
                               logging: Boolean = true,
                               password: String,
                               keyName: String = "nispero",
                               removeAllQueues: Boolean = true,
                               timeout: Int = 72000,
                               mergeQueueThroughput: MergeQueueThroughput = SampleBased(1),
                               generateDot: Boolean = true
                               ) extends MetapastaConfiguration {
}


//mappingWorkers = Group(size = 1, max = 20, instanceType = InstanceType.T1Micro, purchaseModel = SpotAuto)