package ohnosequences.metapasta

import ohnosequences.awstools.s3.ObjectAddress
import ohnosequences.metapasta.reporting.SampleId


object S3Paths {

  def mergedFastq(resultsObject: ObjectAddress, sample: String): ObjectAddress = resultsObject / sample / "reads" / (sample + ".merged.fastq")

  def notMergedFastq(resultsObject: ObjectAddress, sample: String): (ObjectAddress, ObjectAddress) = {
    (resultsObject / sample / "reads" / (sample + ".notMerged1.fastq"),  resultsObject / sample / "reads" / (sample + ".notMerged2.fastq"))
  }

  def mergedNoHitFasta(resultsObject: ObjectAddress, sample: String) : ObjectAddress =    resultsObject / sample / "reads" / (sample + "noHit.fasta")
  def mergedAssignedFasta(resultsObject: ObjectAddress, sample: String, assignmentType: AssignmentType) : ObjectAddress = resultsObject / sample / "reads" / (sample + "." + assignmentType + ".assigned.fasta")
  def mergedNotAssignedFasta(resultsObject: ObjectAddress, sample: String, assignmentType: AssignmentType):ObjectAddress =resultsObject / sample / "reads" / (sample + "." + assignmentType + ".notAssigned.fasta")


  def noHitFastas(readsObject: ObjectAddress, sample: String) : ObjectAddress = readsObject / sample / "noHit"
  def notAssignedFastas(readsObject: ObjectAddress, sample: String, assignmentType: AssignmentType) : ObjectAddress = readsObject / (sample + "###" + assignmentType) / "notAssigned"
  def assignedFastas(readsObject: ObjectAddress, sample: String, assignmentType: AssignmentType): ObjectAddress = readsObject / (sample + "###" + assignmentType) / "assigned"



  def noHitFasta(readsObject: ObjectAddress, chunk: MergedSampleChunk): ObjectAddress = {
    noHitFastas(readsObject, chunk.sample) / (chunk.range._1 + "_" + chunk.range._2 + ".fasta")
  }
  def notAssignedFasta(readsObject: ObjectAddress, chunk: MergedSampleChunk, assignmentType: AssignmentType): ObjectAddress = {
    notAssignedFastas(readsObject, chunk.sample, assignmentType) / (chunk.range._1 + "_" + chunk.range._2 + ".fasta")
  }
  def assignedFasta(readsObject: ObjectAddress, chunk: MergedSampleChunk, assignmentType: AssignmentType): ObjectAddress = {
    assignedFastas(readsObject, chunk.sample, assignmentType) / (chunk.range._1 + "_" + chunk.range._2 + ".fasta")
  }
  
  def blastOut(readsObject: ObjectAddress, chunk: MergedSampleChunk): ObjectAddress = {
    readsObject / chunk.sample / "blast" / (chunk.sample + chunk.range._1 + "_" + chunk.range._2 + ".blast")
  }

}
