package ohnosequences.metapasta

import ohnosequences.metapasta.databases.ReferenceId
import ohnosequences.compota.AWS
import ohnosequences.formats.{RawHeader, FASTQ}

import scala.collection.mutable

class FastasWriter[R <: ReferenceId](aws: AWS, s3Paths: S3Paths, taxonomy: Taxonomy) {

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
    aws.s3.putWholeObject(s3Paths.noHitFasta(chunk), noHitFasta.toString())
    aws.s3.putWholeObject(s3Paths.noTaxIdFasta(chunk, assignmentType), noTaxIdFasta.toString())
    aws.s3.putWholeObject(s3Paths.notAssignedFasta(chunk, assignmentType), notAssignedFasta.toString())
    aws.s3.putWholeObject(s3Paths.assignedFasta(chunk, assignmentType), assignedFasta.toString())
    noHitFasta.clear()
    noTaxIdFasta.clear()
    notAssignedFasta.clear()
    assignedFasta.clear()
  }

}
