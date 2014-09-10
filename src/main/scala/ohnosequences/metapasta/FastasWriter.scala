package ohnosequences.metapasta

import ohnosequences.formats.{RawHeader, FASTQ}
import scala.collection.mutable
import ohnosequences.nisperon.logging.{Logger, S3Logger}
import ohnosequences.nisperon.AWS
import ohnosequences.awstools.s3.ObjectAddress
import ohnosequences.metapasta.reporting.SampleId


case class ChunkId(sample: SampleId, start: Long, end: Long)

object ChunkId {
  def apply(chunk: MergedSampleChunk): ChunkId = ChunkId(SampleId(chunk.sample), chunk.range._1, chunk.range._2)
}

class FastasWriter(aws: AWS, readsDirectory: ObjectAddress, nodeRetriever: NodeRetriever) {
  val noHitFasta = new mutable.StringBuilder()
  val noTaxIdFasta = new mutable.StringBuilder()
  val notAssignedFasta = new mutable.StringBuilder()
  val assignedFasta = new mutable.StringBuilder()

  def fastaHeader(sampleId: String, taxon: Taxon, refIds: Set[RefId], reason: Option[String] = None): String = {

    val (taxname, rank) = try {
      val node = nodeRetriever.nodeRetriever.getNCBITaxonByTaxId(taxon.taxId)
      (node.getScientificName(), node.getRank())
    } catch {
      case t: Throwable => ("na", "na")
    }
    sampleId + "|" + taxname + "|" + taxon.taxId + "|" + rank + "|" + refIds.foldLeft("")(_ + "|" + _.refId) + reason.map("|" + _).getOrElse("")
  }

  def write(sample: SampleId, read: FASTQ[RawHeader], readId: ReadId, assignment: Assignment) {
    assignment match {
      case TaxIdAssignment(taxon, refIds, _, _) => {
        assignedFasta.append(read.toFasta(fastaHeader(sample.id, taxon, refIds)))
        assignedFasta.append(System.lineSeparator())
      }
      case NoTaxIdAssignment(refId) => {
        noTaxIdFasta.append(read.toFasta)
        noTaxIdFasta.append(System.lineSeparator())
      }
      case NotAssigned(reason, refIds, taxIds) => {
        notAssignedFasta.append(read.toFasta(fastaHeader(sample.id, Taxon(""), refIds, Some(reason))))
        noTaxIdFasta.append(System.lineSeparator())
      }
    }
  }

  //hohit
  def writeNoHit(read: FASTQ[RawHeader], readId: ReadId) {
    noHitFasta.append(read.toFasta)
    noHitFasta.append(System.lineSeparator())

  }

  def uploadFastas(chunk: ChunkId, assignmentType: AssignmentType) {
    aws.s3.putWholeObject(S3Paths.noHitFasta(readsDirectory, chunk), noHitFasta.toString())
    aws.s3.putWholeObject(S3Paths.noTaxIdFasta(readsDirectory, chunk, assignmentType), noTaxIdFasta.toString())
    aws.s3.putWholeObject(S3Paths.notAssignedFasta(readsDirectory, chunk, assignmentType), notAssignedFasta.toString())
    aws.s3.putWholeObject(S3Paths.assignedFasta(readsDirectory, chunk, assignmentType), assignedFasta.toString())
    noHitFasta.clear()
    noTaxIdFasta.clear()
    notAssignedFasta.clear()
    assignedFasta.clear()
  }

}
