package ohnosequences.nisperon.bundles

import ohnosequences.statika._
import ohnosequences.statika.aws._
import ohnosequences.awstools.s3.ObjectAddress

//case class NisperonMetadata(s3Bucket: String)

//todo add autotermination
object NisperonAMI extends AMI[NisperonMetadata]("ami-5256b825", "2013.09") {

 // type Metadata = NisperonMetadata

  def userScript(
                  metadata: NisperonMetadata
                  , distName: String
                  , bundleName: String
                  , creds: AWSCredentials = RoleCredentials
                  ): String = {


    val raw = """
                |#!/bin/sh
                |cd /root
                |exec &> log.txt
                |yum install java-1.7.0-openjdk.x86_64 -y
                |alternatives --install /usr/bin/java java /usr/lib/jvm/jre-1.7.0-openjdk.x86_64/bin/java 20000
                |alternatives --auto java
                |echo " -- Installing git -- "
                |echo
                |yum install git -y
                |
                |echo
                |echo " -- Installing s3cmd -- "
                |echo
                |git clone https://github.com/s3tools/s3cmd.git
                |cd s3cmd/
                |python setup.py install
                |echo "[default]" > /root/.s3cfg
                |
                |cd /root
                |
                |s3cmd --config /root/.s3cfg get s3://$bucket$/$key$
                |
                |cd $workingDir$
                |
                |java -jar /root/$jarFile$ $component$ $name$
                |
              """.stripMargin
      .replace("$bucket$", metadata.jarAddress.bucket)
      .replace("$key$", metadata.jarAddress.key)
      .replace("$jarFile$", getFileName(metadata.jarAddress.key))
      .replace("$component$", metadata.component)
      .replace("$name$", metadata.nisperoId)
      .replace("$workingDir$", metadata.workingDir)

    fixLineEndings(raw)
  }

  def fixLineEndings(s: String): String = s.replaceAll("\\r\\n", "\n").replaceAll("\\r", "\n")

  def getFileName(s: String) = s.substring(s.lastIndexOf("/") + 1)

}
