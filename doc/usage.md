## metapasta usage

### installation

#### cs and g8
[`conscript`](https://github.com/n8han/conscript#installation) and [`g8`](https://github.com/n8han/giter8#installation) should be installed.

To create download a template with metapasta project type:

```
g8 ohnosequences/metapasta
```


#### credentials

AWS credentials should be placed in `~/metapasta.credentials`, the file should have a format like this

```bash
accessKey = AKIAIG23IDH2AEPBEFVA
#for example: accessKey = AKIAIG23IDH2AEPBEFVA

secretKey = AZpGhgq6i4+m+TRXJ0W8nYmRJY3ejr5p5DQULTci
#for example: secretKey = AZpGhgq6i4+m+TRXJ0W8nYmRJY3ejr5p5DQULTci
```

### configuration

After downloading the template, all metapasta parameters can be installed.

#### samples
Reads should be either single or paired ended with intersection. Example for mock community:

```scala
object mockSamples {
  val testBucket = "metapasta-test"

  val ss1 = "SRR172902"
  val s1 = PairedSample(ss1, ObjectAddress(testBucket, "mock/" + ss1 + ".fastq"), ObjectAddress(testBucket, "mock/" + ss1 + ".fastq"))

  val ss2 = "SRR172903"
  val s2 = PairedSample(ss2, ObjectAddress(testBucket, "mock/" + ss2 + ".fastq"), ObjectAddress(testBucket, "mock/" + ss2 + ".fastq"))

  val samples = List(s1, s2)
}
```

> FASTQ files should have solexa quality format.

#### configuration
Configuration depends on mapping tool (BLAST or LAST). Some useful parameters:

##### mappingWorkers  
configuration of auto scaling group with mapping workers. It is recommended to use `T1Micro` for BLAST and `M1Large` for LAST. In general BLAST performs mapping quite slow, so for it you will need hundred of mapping workers. Example:

```scala
mappingWorkers = Group(size = 200, max = 200, instanceType = InstanceType.T1Micro, purchaseModel = SpotAuto)
```

LAST requires not so a lot instances but they should have at least 3GB RAM (for nt.16S database).

##### keyName
Name of ssh key that can be used for connecting to instances.

##### timeout
Global timeout in seconds for metapasta

##### database
Index of reference database. Metabasta bundled with nt.16s database, but other databased can be implemented (see https://github.com/ohnosequences/metapasta/blob/master/src/main/scala/ohnosequences/metapasta/Database.scala).


##### other parameters
https://github.com/ohnosequences/metapasta/blob/master/src/main/scala/ohnosequences/metapasta/MetapastaConfiguration.scala



### commands 

##### publish

```
sbt publish
```


##### launching and adding tasks

```
sbt "run run"
```


##### termination

*metapasta* will automatically when all assignments work will finish. Although it is possible to do manual undeploy in case if autotermination doesn't work

```
sbt "run undeploy"
```

in case if it doesn't work:

```
sbt "run undeploy force"
```

##### adding tasks

```
sbt "run add tasks"
```

##### changing size of workers group

```
sbt "run map size <number>"
```






