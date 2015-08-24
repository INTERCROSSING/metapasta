package ohnosequences.metapasta

import com.amazonaws.services.s3.AmazonS3
import ohnosequences.awstools.s3.ObjectAddress

/**
 * Created by evdokim on 24/08/2015.
 */
class S3Splitter(s3: AmazonS3, address: ObjectAddress, chunksSize: Long) {

  def objectSize(): Long = {
    s3.getObjectMetadata(address.bucket, address.key).getContentLength
  }

  def chunks(): List[(Long, Long)] = {

    val size = objectSize()

    val starts = 0L until size by chunksSize
    val ends = starts.map { start =>
      math.min(start + chunksSize - 1, size - 1)
    }

    starts.zip(ends).toList
  }

}
