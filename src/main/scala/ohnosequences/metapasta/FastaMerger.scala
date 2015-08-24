package ohnosequences.metapasta

import ohnosequences.awstools.s3.{LoadingManager, ObjectAddress}
import ohnosequences.compota.AWS

import java.io.{PrintWriter, File}

import ohnosequences.logging.ConsoleLogger


class FastaMerger(aws: AWS, readObject: ObjectAddress, s3Paths: S3Paths, samples: List[SampleId]) {

  val logger = new ConsoleLogger("fast merger")
  val lm = aws.s3.createLoadingManager()


  def merge() {
    for (sample <- samples) {
      logger.info("merging noHits fastas for sample " + sample)
      rawMerge(s3Paths.noHitFastas(sample), s3Paths.mergedNoHitFasta(sample), lm)



      for (asType <- List(LCA, BBH)) {

        logger.info("merging noTaxIds fastas for sample " + sample.id + " for " + asType)
        rawMerge(s3Paths.noTaxIdFastas(sample, asType), s3Paths.mergedNoTaxIdFasta(sample, asType), lm)

        logger.info("merging notAssigned fastas for sample " + sample.id + " for " + asType)
        rawMerge(s3Paths.notAssignedFastas(sample, asType), s3Paths.mergedNotAssignedFasta(sample, asType), lm)

        logger.info("merging assigned fastas for sample " + sample.id + " for " + asType)
        rawMerge(s3Paths.assignedFastas(sample, asType), s3Paths.mergedAssignedFasta(sample, asType), lm)
      }

    }
  }

  def rawMerge(address: ObjectAddress, dst: ObjectAddress, lm: LoadingManager) {
    val objects = aws.s3.listObjects(address.bucket, address.key)
    var c = objects.size
    val res = new File("res")
    val pw = new PrintWriter(res)

    if (objects.isEmpty) {
      logger.warn("couldn't find files in: " + address)
    }
    for (obj <- objects) {
      if (c % 100 == 0) {
        logger.info("merger: " + c + " objects left")
      }
      val s = try {
        aws.s3.readWholeObject(obj)
      } catch {
        case t: Throwable => logger.error("couldn't read from object: " + obj + " skipping"); ""
      }
      pw.println(s)
      c -= 1
    }
    pw.close()
    logger.info("uploading results to: "  + dst)
    lm.upload(dst, res)

  }

}
