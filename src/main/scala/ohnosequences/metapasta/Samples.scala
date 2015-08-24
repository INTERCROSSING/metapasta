package ohnosequences.metapasta

import ohnosequences.awstools.s3.ObjectAddress

case class PairedSample(name: String, fastq1: ObjectAddress, fastq2: ObjectAddress) {
  def id = SampleId(name)
}

case class SampleId(id: String)

case class SampleTag(tag: String)

case class MergedSampleChunk(fastq: ObjectAddress, sample: SampleId, start: Long, end: Long) {
  def chunkId: ChunkId = ChunkId(sample, start, end)
}

case class ChunkId(sample: SampleId, start: Long, end: Long)

