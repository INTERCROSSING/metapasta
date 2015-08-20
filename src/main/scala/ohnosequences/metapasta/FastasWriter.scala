package ohnosequences.metapasta

import ohnosequences.metapasta.databases.ReferenceId
import ohnosequences.metapasta.reporting.SampleId
import ohnosequences.compota.AWS
import ohnosequences.formats.{RawHeader, FASTQ}
import ohnosequences.awstools.s3.ObjectAddress

import scala.collection.mutable

case class ChunkId(sample: SampleId, start: Long, end: Long)

object ChunkId {
  def apply(chunk: MergedSampleChunk): ChunkId = ChunkId(SampleId(chunk.sample), chunk.range._1, chunk.range._2)
}

class FastasWriter[R <: ReferenceId](aws: AWS, readsDirectory: ObjectAddress, taxonomy: Taxonomy) {

  val noHitFasta = new mutable.StringBuilder()
  val noTaxIdFasta = new mutable.StringBuilder()
  val notAssignedFasta = new mutable.StringBuilder()
  val assignedFasta = new mutable.StringBuilder()

  def fastaHeader(sampleId: String, taxon: Taxon, refIds: Set[R], reason: Option[String]): String = {

    val (taxname, taxonId, rank) = reason match {
      case Some(r) => {
        (r, "", "")
      }
      case None => {
        taxonomy.getTaxonInfo(taxon) match {
          case None => ("", taxon.taxId, "")
          case Some(taxonInfo) => (taxonInfo.scientificName, taxon.taxId, taxonInfo.rank.toString)
        }
      }
    }


    sampleId + "|" + taxname + "|" + taxonId + "|" + rank + "|" + refIds.foldLeft("")(_ + "_" + _.id) + "|"
  }



  def write(sample: SampleId, read: FASTQ[RawHeader], readId: ReadId, assignment: Assignment[R]) {
    assignment match {
      case TaxIdAssignment(taxon, refIds, _, _, _) => {
        assignedFasta.append(read.toFasta(fastaHeader(sample.id, taxon, refIds, None)))
        assignedFasta.append(System.lineSeparator())
      }
      case NoTaxIdAssignment(refIds) => {
        noTaxIdFasta.append(read.toFasta(fastaHeader(sample.id, Taxon(""), refIds, Some("NoTaxIdCorrespondence"))))
        noTaxIdFasta.append(System.lineSeparator())
      }
      case NotAssigned(reason, refIds, taxIds) => {
        notAssignedFasta.append(read.toFasta(fastaHeader(sample.id, Taxon(""), refIds, Some("LCAfiltered"))))
        notAssignedFasta.append(System.lineSeparator())
      }
    }
  }

  //hohit
  def writeNoHit(read: FASTQ[RawHeader], readId: ReadId, sample: SampleId) {
    noHitFasta.append(read.toFasta(fastaHeader(sample.id, Taxon(""), Set[R](), Some("NoHits"))))
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
